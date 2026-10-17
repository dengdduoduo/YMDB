#!/usr/bin/env python3
"""
Single-run benchmark: test one DB (YB or MDB) on one dataset, one query count.
Saves incremental JSON results — safe to interrupt and review.

Usage:
  # YB single-node (random queries)
  python3 run_single_benchmark.py --db yb --dataset ds100 --num-queries 100

  # YB with syntax-covering queries (1 per type)
  python3 run_single_benchmark.py --db yb --dataset ds10000 --num-queries 22 --query-type cover --tservers 2

  # YB 2 tserver nodes
  python3 run_single_benchmark.py --db yb --dataset ds100 --num-queries 100 --tservers 2

  # MDB
  python3 run_single_benchmark.py --db mdb --dataset ds100 --num-queries 100
"""

import subprocess
import json
import time
import os
import sys
import statistics
import random
import argparse
import signal
import shutil
import requests

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

YB_CLI = "/home/nonitem/yugabyte-db/build/release-clang21-dynamic-ninja/bin/mdb_cli"
YB_MASTERS = "127.0.0.1:7100"
YB_CTL = "/home/nonitem/yugabyte-db/bin/yb-ctl"
MDB_BIN = "/home/nonitem/MillenniumDB/build/bin/mdb"
MDB_PORT = 9501

PREFIXES = """PREFIX ex: <http://example.org/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
"""


def run_cmd(cmd, timeout=120, cwd=None):
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True,
                                timeout=timeout, cwd=cwd)
        return result.stdout, result.stderr, result.returncode
    except subprocess.TimeoutExpired:
        return "", "TIMEOUT", -1


def compute_stats(times):
    if not times:
        return {"count": 0, "min": 0, "max": 0, "mean": 0,
                "p50": 0, "p90": 0, "p95": 0, "p99": 0}
    times_sorted = sorted(times)
    n = len(times_sorted)

    def pct(p):
        k = (n - 1) * p
        f = int(k)
        c = k - f
        if f + 1 < n:
            return times_sorted[f] + c * (times_sorted[f + 1] - times_sorted[f])
        return times_sorted[f]

    return {
        "count": n, "min": min(times_sorted), "max": max(times_sorted),
        "mean": statistics.mean(times_sorted),
        "p50": pct(0.50), "p90": pct(0.90), "p95": pct(0.95), "p99": pct(0.99),
    }


# ─── YB cluster management ───

def yb_cluster_status():
    out, _, rc = run_cmd("./bin/yb-ctl status 2>&1", timeout=10, cwd="/home/nonitem/yugabyte-db")
    return out, rc


def yb_stop_cluster():
    run_cmd("./bin/yb-ctl destroy 2>&1", timeout=60, cwd="/home/nonitem/yugabyte-db")


def yb_start_cluster(num_tservers=1):
    """Start YB cluster with specified number of tservers."""
    yb_stop_cluster()
    time.sleep(3)

    if num_tservers <= 1:
        out, err, rc = run_cmd(
            "./bin/yb-ctl start "
            "--tserver_flags 'cql_proxy_bind_address=127.0.0.1:9042' "
            "--master_flags 'replication_factor=1' 2>&1",
            timeout=60, cwd="/home/nonitem/yugabyte-db")
        if rc != 0:
            raise RuntimeError(f"yb-ctl start failed: {err or out}")
        print("  Cluster started: 1 master + 1 tserver")
        return

    # Multi-tserver: start with 1 first, then add nodes
    out, err, rc = run_cmd(
        "./bin/yb-ctl start "
        "--tserver_flags 'cql_proxy_bind_address=127.0.0.1:9042' "
        "--master_flags 'replication_factor=1' 2>&1",
        timeout=60, cwd="/home/nonitem/yugabyte-db")
    if rc != 0:
        raise RuntimeError(f"yb-ctl start failed: {err or out}")
    print("  Cluster base started: 1 master + 1 tserver")

    for i in range(1, num_tservers):
        out, err, rc = run_cmd(
            "./bin/yb-ctl add_node "
            f"--tserver_flags 'cql_proxy_bind_address=127.0.0.1:{9042+i+1}' 2>&1",
            timeout=60, cwd="/home/nonitem/yugabyte-db")
        if rc != 0:
            print(f"  WARNING: add_node #{i+1} failed: {err or out}")
        else:
            print(f"  Added tserver #{i+1}")
        time.sleep(3)

    print(f"  Cluster: 1 master + {num_tservers} tservers")

    time.sleep(10)
    last_err = ""
    for attempt in range(24):
        out, err, rc = run_cmd(
            f"{YB_CLI} --mdb_masters={YB_MASTERS} --mdb_keyspace=system query 'SELECT * WHERE {{ ?s ?p ?o }} LIMIT 1' 2>&1",
            timeout=10)
        if rc == 0:
            print(f"  Cluster ready (attempt {attempt+1})")
            break
        last_err = (err or out)[:200]
        time.sleep(5)
    else:
        raise RuntimeError(f"Cluster not ready after 24 attempts: {last_err}")


def yb_import(dataset_name):
    nt_file = os.path.join(SCRIPT_DIR, f"dataset_{dataset_name}.nt")
    keyspace = f"bench_{dataset_name}"
    print(f"  YB import {nt_file} → keyspace={keyspace}")
    t0 = time.time()
    out, err, rc = run_cmd(
        f"{YB_CLI} --mdb_masters={YB_MASTERS} --mdb_keyspace={keyspace} "
        f"import {os.path.abspath(nt_file)} 2>&1",
        timeout=3600)
    elapsed = time.time() - t0
    if rc != 0:
        raise RuntimeError(f"YB import FAIL ({elapsed:.1f}s): {(err or out)[:500]}")
    print(f"  YB import OK ({elapsed:.1f}s)")
    return keyspace, elapsed


def yb_query(keyspace, query):
    full_query = PREFIXES + "\n" + query
    t0 = time.time()
    out, err, rc = run_cmd(
        f"{YB_CLI} --mdb_masters={YB_MASTERS} --mdb_keyspace={keyspace} "
        f"query '{full_query}' 2>&1",
        timeout=600)
    elapsed = time.time() - t0
    if rc != 0:
        return None, elapsed, (err or out)[:200]
    rows = []
    cur = []
    for line in out.strip().split("\n"):
        line = line.strip()
        if line == "---":
            if cur:
                rows.append(tuple(cur)); cur = []
            continue
        if "\t" in line:
            parts = line.split("\t", 1)
            if len(parts) == 2:
                cur.append(parts[1])
    if cur:
        rows.append(tuple(cur))
    return rows, elapsed, None


# ─── MDB management ───

class MdbProc:
    def __init__(self, db_folder):
        self.db_folder = db_folder
        self.proc = None

    def start(self):
        subprocess.run(f"fuser -k {MDB_PORT}/tcp 2>/dev/null", shell=True, timeout=5)
        time.sleep(3)
        self.proc = subprocess.Popen(
            [MDB_BIN, "server", self.db_folder, "--port", str(MDB_PORT), "--browser", "false"],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        time.sleep(3)
        if self.proc.poll() is not None:
            out = ""
            try: out = self.proc.stdout.read().decode()
            except: pass
            raise RuntimeError(f"MDB server exited early: {out[:300]}")
        subprocess.run(
            f"for i in $(seq 1 10); do curl -s --max-time 2 -o /dev/null http://localhost:{MDB_PORT}/sparql 2>/dev/null && break; sleep 1; done",
            shell=True, timeout=15)

    def stop(self):
        global _mdb_session
        if _mdb_session:
            _mdb_session.close()
            _mdb_session = None
        if self.proc:
            self.proc.send_signal(signal.SIGTERM)
            try: self.proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait(timeout=5)
            except Exception:
                self.proc.kill()
                try: self.proc.wait(timeout=5)
                except: pass
            self.proc = None
        subprocess.run(f"fuser -k {MDB_PORT}/tcp 2>/dev/null", shell=True, timeout=5)
        time.sleep(2)

    def __enter__(self):
        self.start(); return self
    def __exit__(self, *args):
        self.stop()


def mdb_import(dataset_name, db_folder):
    shutil.rmtree(db_folder, ignore_errors=True)
    nt_file = os.path.join(SCRIPT_DIR, f"dataset_{dataset_name}.nt")
    print(f"  MDB import {nt_file} → {db_folder}")
    t0 = time.time()
    out, err, rc = run_cmd(
        f"{MDB_BIN} import {os.path.abspath(nt_file)} {db_folder} --format nt 2>&1",
        timeout=600)
    elapsed = time.time() - t0
    if rc != 0:
        raise RuntimeError(f"MDB import FAIL ({elapsed:.1f}s): {(err or out)[:500]}")
    print(f"  MDB import OK ({elapsed:.1f}s)")
    return elapsed


_mdb_session = None

def mdb_query(query):
    global _mdb_session
    if _mdb_session is None:
        _mdb_session = requests.Session()
        _mdb_session.headers.update({"Content-Type": "application/sparql-query"})
        adapter = requests.adapters.HTTPAdapter(pool_connections=1, pool_maxsize=10)
        _mdb_session.mount("http://", adapter)

    full_query = PREFIXES + "\n" + query
    t0 = time.time()
    try:
        resp = _mdb_session.post(
            f"http://localhost:{MDB_PORT}/sparql",
            data=full_query.encode("utf-8"),
            timeout=300)
        elapsed = time.time() - t0
        if resp.status_code != 200:
            return None, elapsed, f"HTTP {resp.status_code}: {resp.text[:100]}"
        data = resp.json()
        rows = []
        for bind in data.get("results", {}).get("bindings", []):
            row = tuple(bind.get(v, {}).get("value", "") for v in data["head"]["vars"])
            rows.append(row)
        return rows, elapsed, None
    except requests.exceptions.Timeout:
        elapsed = time.time() - t0
        return None, elapsed, "TIMEOUT"
    except requests.exceptions.ConnectionError as e:
        elapsed = time.time() - t0
        return None, elapsed, f"CONNECTION_ERROR: {str(e)[:150]}"
    except Exception as e:
        elapsed = time.time() - t0
        return None, elapsed, f"ERROR: {str(e)[:150]}"


# ─── Main runner ───

def run_benchmark(args):
    random.seed(42)
    ds = args.dataset
    qn = args.num_queries
    db = args.db

    # Load or generate query pool
    qtype = getattr(args, 'query_type', 'random')
    pool_file = os.path.join(SCRIPT_DIR, "queries", f"{qtype}_{ds}.json")
    if not os.path.exists(pool_file):
        if qtype == "cover":
            print(f"Generating cover queries for {ds}...")
            subprocess.run([sys.executable, os.path.join(SCRIPT_DIR,
                            "generate_cover_queries.py")], check=True)
        else:
            print(f"Generating query pool for {ds}...")
            subprocess.run([sys.executable, os.path.join(SCRIPT_DIR,
                            "generate_random_queries.py"), ds, "2000"], check=True)
    with open(pool_file) as f:
        pool = json.load(f)
    print(f"Loaded {len(pool)} queries from pool")

    # Sample queries (with replacement if needed)
    if qn > len(pool):
        queries = random.choices(pool, k=qn)
    elif qtype == "cover":
        queries = list(pool)
    else:
        queries = random.sample(pool, qn)

    # Output file
    out_dir = os.path.join(SCRIPT_DIR, "bench_results")
    os.makedirs(out_dir, exist_ok=True)
    tag = f"{ds}_q{qn}"
    if db == "yb" and args.tservers:
        tag += f"_ts{args.tservers}"
    out_path = os.path.join(out_dir, f"single_{db}_{tag}.json")
    progress_path = out_path + ".progress"

    # Load existing progress
    results = {"db": db, "dataset": ds, "num_queries": qn,
               "tservers": args.tservers if db == "yb" else 0,
               "import_time_s": 0, "times_ms": [], "errors": [],
               "row_counts": [], "details": []}
    start_idx = 0
    if os.path.exists(progress_path):
        with open(progress_path) as f:
            saved = json.load(f)
            results.update(saved)
            start_idx = len(results["details"])
            print(f"Resuming from query {start_idx+1}/{qn}")

    # Setup DB
    if db == "yb":
        yb_start_cluster(args.tservers)
        keyspace, import_time = yb_import(ds)
        results["import_time_s"] = import_time
    else:
        db_folder = f"/tmp/mdb_single_{ds}"
        import_time = mdb_import(ds, db_folder)
        results["import_time_s"] = import_time
        server = MdbProc(db_folder)
        server.start()

    try:
        mdb_query_count = 0
        for i in range(start_idx, qn):
            if db == "mdb" and mdb_query_count >= 50:
                server.stop()
                server = MdbProc(db_folder)
                server.start()
                mdb_query_count = 0
                print(f"  Server restarted at query {i+1}")

            qobj = queries[i]
            query = qobj["query"]
            detail = {"id": qobj["id"], "template": qobj["template"],
                      "category": qobj["category"]}

            if db == "yb":
                rows, elapsed, err = yb_query(keyspace, query)
            else:
                rows, elapsed, err = mdb_query(query)
                mdb_query_count += 1

            elapsed_ms = elapsed * 1000
            detail["time_ms"] = elapsed_ms

            if err:
                detail["error"] = err
                results["errors"].append(i)
            else:
                results["times_ms"].append(elapsed_ms)
                results["row_counts"].append(len(rows) if rows else 0)

            results["details"].append(detail)

            # Save progress every 10 queries or at end
            if (i + 1) % 10 == 0 or i == qn - 1:
                with open(progress_path, "w") as f:
                    json.dump(results, f, indent=2)

            if (i + 1) % max(1, qn // 10) == 0:
                pct = 100 * (i + 1) / qn
                print(f"  Progress: {i+1}/{qn} ({pct:.0f}%)  "
                      f"errors={len(results['errors'])}  "
                      f"last={elapsed_ms:.1f}ms")

    finally:
        if db == "mdb":
            server.stop()

    # Compute stats
    stats = compute_stats(results["times_ms"])
    results["stats"] = stats

    # Final save
    with open(out_path, "w") as f:
        json.dump(results, f, indent=2)
    if os.path.exists(progress_path):
        os.remove(progress_path)

    print(f"\n  === RESULTS: {db} {ds} q={qn} ===")
    print(f"  Total queries: {qn}")
    print(f"  Errors: {len(results['errors'])}")
    print(f"  Valid times: {len(results['times_ms'])}")
    print(f"  p50={stats['p50']:.1f}ms  p90={stats['p90']:.1f}ms  "
          f"p95={stats['p95']:.1f}ms  p99={stats['p99']:.1f}ms")
    print(f"  min={stats['min']:.1f}ms  max={stats['max']:.1f}ms  "
          f"mean={stats['mean']:.1f}ms")
    print(f"  Saved: {out_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", required=True, choices=["yb", "mdb"])
    parser.add_argument("--dataset", required=True, help="ds100/ds200/ds500/ds1000/ds2000")
    parser.add_argument("--num-queries", type=int, required=True)
    parser.add_argument("--tservers", type=int, default=1, help="YB tserver count")
    parser.add_argument("--query-type", default="random", choices=["random", "cover"],
                        help="random=random samples, cover=one per syntax type")
    args = parser.parse_args()
    run_benchmark(args)
