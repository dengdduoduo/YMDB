#!/usr/bin/env python3
"""
Run ALL 131 coverage queries against YB (2ts) and MDB on ds2000,
measuring per-query wall-clock time for comparison.
"""

import subprocess
import json
import time
import os
import sys
import statistics
import ast

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
YB_CLI = "/home/nonitem/yugabyte-db/build/debug-clang21-dynamic-ninja/bin/mdb_cli"
YB_MASTERS = "127.0.0.1:7100"
YB_CTL = "/home/nonitem/yugabyte-db/bin/yb-ctl"
MDB_BIN = "/home/nonitem/MillenniumDB/build/bin/mdb"
MDB_PORT = 9501

PREFIXES = """PREFIX ex: <http://example.org/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX mdbfn: <https://mdb.imfd.cl/function#>
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
        return {"min": 0, "max": 0, "mean": 0, "p50": 0, "p90": 0, "p95": 0, "p99": 0}
    s = sorted(times)
    n = len(s)
    def pct(p):
        k = (n - 1) * p
        f = int(k)
        c = k - f
        if f + 1 < n:
            return s[f] + c * (s[f + 1] - s[f])
        return s[f]
    return {
        "min": min(s), "max": max(s), "mean": statistics.mean(s),
        "p50": pct(0.50), "p90": pct(0.90), "p95": pct(0.95), "p99": pct(0.99),
    }

# ─── Extract queries from test_sparql_coverage.py ───

def extract_all_queries():
    coverage_path = os.path.join(SCRIPT_DIR, "test_sparql_coverage.py")
    with open(coverage_path) as f:
        src = f.read()

    tree = ast.parse(src)
    all_test_queries = []

    class QueryCollector(ast.NodeVisitor):
        def visit_Call(self, node):
            # Look for: all_test_queries.append((name, expected, [(label, query), ...]))
            if (isinstance(node.func, ast.Attribute) and
                node.func.attr == 'append' and
                isinstance(node.func.value, ast.Name) and
                node.func.value.id == 'all_test_queries'):
                if len(node.args) == 1:
                    arg = node.args[0]
                    if isinstance(arg, ast.Tuple) and len(arg.elts) == 3:
                        try:
                            name_str = ast.literal_eval(arg.elts[0])
                            expected = ast.literal_eval(arg.elts[1])
                            qlist_node = arg.elts[2]
                            if isinstance(qlist_node, ast.List):
                                qlist = ast.literal_eval(qlist_node)
                                all_test_queries.append((name_str, expected, qlist))
                        except (ValueError, SyntaxError):
                            pass
            self.generic_visit(node)

    QueryCollector().visit(tree)

    queries = []
    idx = 0
    for name, expected_success, qlist in all_test_queries:
        for label, qtext in qlist:
            idx += 1
            queries.append({
                "id": idx,
                "category": name,
                "label": label,
                "query": qtext.strip(),
            })

    return queries

# ─── YB cluster ───

def yb_start_cluster(num_tservers=2):
    run_cmd(f"{YB_CTL} destroy 2>&1", timeout=60, cwd="/home/nonitem/yugabyte-db")
    time.sleep(3)

    out, err, rc = run_cmd(
        f"{YB_CTL} start --tserver_flags 'cql_proxy_bind_address=127.0.0.1:9042' "
        f"--master_flags 'replication_factor=1' 2>&1",
        timeout=60, cwd="/home/nonitem/yugabyte-db")
    if rc != 0:
        raise RuntimeError(f"yb-ctl start failed: {err or out}")
    print("  Base cluster: 1 master + 1 tserver")

    for i in range(1, num_tservers):
        out, err, rc = run_cmd(
            f"{YB_CTL} add_node --tserver_flags 'cql_proxy_bind_address=127.0.0.1:{9042+i+1}' 2>&1",
            timeout=60, cwd="/home/nonitem/yugabyte-db")
        if rc != 0:
            print(f"  WARNING: add_node #{i+1} failed: {err or out}")
        else:
            print(f"  Added tserver #{i+1}")
        time.sleep(3)

    print(f"  Cluster: 1 master + {num_tservers} tservers")
    time.sleep(10)

    for attempt in range(24):
        out, err, rc = run_cmd(
            f"{YB_CLI} --mdb_masters={YB_MASTERS} --mdb_keyspace=system query "
            f"'SELECT * WHERE {{ ?s ?p ?o }} LIMIT 1' 2>&1", timeout=10)
        if rc == 0:
            print(f"  Cluster ready (attempt {attempt+1})")
            break
        time.sleep(5)
    else:
        raise RuntimeError(f"Cluster not ready after 24 attempts")

def yb_import(ds):
    nt_file = os.path.join(SCRIPT_DIR, f"dataset_{ds}.nt")
    keyspace = f"bench_all_{ds}"
    print(f"  YB import {nt_file} → keyspace={keyspace}")
    t0 = time.time()
    out, err, rc = run_cmd(
        f"{YB_CLI} --mdb_masters={YB_MASTERS} --mdb_keyspace={keyspace} "
        f"import {os.path.abspath(nt_file)} 2>&1", timeout=3600)
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
        f"query '{full_query}' 2>&1", timeout=600)
    elapsed = time.time() - t0
    if rc != 0:
        return None, elapsed, (err or out)[:200]
    rows = 0
    for line in out.strip().split("\n"):
        if line.strip() == "---":
            rows += 1
    return rows, elapsed, None

# ─── MDB ───

import signal
import shutil

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
            f"for i in $(seq 1 10); do curl -s --max-time 2 -o /dev/null "
            f"http://localhost:{MDB_PORT}/sparql 2>/dev/null && break; sleep 1; done",
            shell=True, timeout=15)

    def stop(self):
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

def mdb_import(ds, db_folder):
    shutil.rmtree(db_folder, ignore_errors=True)
    nt_file = os.path.join(SCRIPT_DIR, f"dataset_{ds}.nt")
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

def mdb_query(query):
    import requests
    full_query = PREFIXES + "\n" + query
    t0 = time.time()
    try:
        resp = requests.post(
            f"http://localhost:{MDB_PORT}/sparql",
            data=full_query.encode("utf-8"),
            headers={"Content-Type": "application/sparql-query"},
            timeout=300)
        elapsed = time.time() - t0
        if resp.status_code != 200:
            return None, elapsed, f"HTTP {resp.status_code}"
        data = resp.json()
        rows = len(data.get("results", {}).get("bindings", []))
        return rows, elapsed, None
    except Exception as e:
        elapsed = time.time() - t0
        return None, elapsed, str(e)[:150]

# ─── Main ───

def main():
    print("=" * 70)
    print("  ds2000 + 2TS 全查询用时对比实验  (YB vs MDB)")
    print("=" * 70)

    queries = extract_all_queries()
    print(f"Extracted {len(queries)} queries from test_sparql_coverage.py")

    results = []

    # ─── YB (2 tservers) ───
    print("\n" + "=" * 70)
    print("  Phase 1: YugabyteDB (2 tablet servers)")
    print("=" * 70)
    yb_start_cluster(2)
    keyspace, import_time = yb_import("ds2000")
    print(f"\n  Running {len(queries)} queries against YB...")

    yb_times = []
    yb_errors = 0
    for idx, q in enumerate(queries, 1):
        rows, elapsed, err = yb_query(keyspace, q["query"])
        elapsed_ms = elapsed * 1000

        if err:
            yb_errors += 1
            results.append({
                "id": q["id"], "category": q["category"], "label": q["label"],
                "yb_ms": None, "mdb_ms": None, "yb_error": err, "mdb_error": None
            })
        else:
            yb_times.append(elapsed_ms)
            results.append({
                "id": q["id"], "category": q["category"], "label": q["label"],
                "yb_ms": elapsed_ms, "mdb_ms": None, "yb_error": None, "mdb_error": None
            })

        if idx % 20 == 0:
            yb_stats = compute_stats(yb_times) if yb_times else {}
            print(f"  [{idx}/{len(queries)}] last={elapsed_ms:.1f}ms  "
                  f"p50={yb_stats.get('p50',0):.1f}ms  errors={yb_errors}")

    yb_stats = compute_stats(yb_times)
    print(f"\n  YB done: {len(queries)} queries, {yb_errors} errors, "
          f"p50={yb_stats['p50']:.1f}ms p90={yb_stats['p90']:.1f}ms "
          f"p95={yb_stats['p95']:.1f}ms p99={yb_stats['p99']:.1f}ms")

    # ─── MDB ───
    print("\n" + "=" * 70)
    print("  Phase 2: MillenniumDB")
    print("=" * 70)
    db_folder = "/tmp/mdb_all_ds2000"
    import_time_mdb = mdb_import("ds2000", db_folder)
    server = MdbProc(db_folder)
    server.start()

    print(f"\n  Running {len(queries)} queries against MDB...")
    import requests
    mdb_times = []
    mdb_errors = 0
    mdb_query_count = 0

    for idx, q in enumerate(queries, 1):
        # Restart MDB every 50 queries to avoid memory pressure
        if mdb_query_count >= 50:
            server.stop()
            server = MdbProc(db_folder)
            server.start()
            mdb_query_count = 0
            print(f"  Server restarted at query {idx}")

        rows, elapsed, err = mdb_query(q["query"])
        elapsed_ms = elapsed * 1000
        mdb_query_count += 1

        ri = results[idx - 1]
        if err:
            mdb_errors += 1
            ri["mdb_error"] = err
        else:
            mdb_times.append(elapsed_ms)
            ri["mdb_ms"] = elapsed_ms

        if idx % 20 == 0:
            mdb_stats = compute_stats(mdb_times) if mdb_times else {}
            print(f"  [{idx}/{len(queries)}] last={elapsed_ms:.1f}ms  "
                  f"p50={mdb_stats.get('p50',0):.1f}ms  errors={mdb_errors}")

    server.stop()
    mdb_stats = compute_stats(mdb_times)
    print(f"\n  MDB done: {len(queries)} queries, {mdb_errors} errors, "
          f"p50={mdb_stats['p50']:.1f}ms p90={mdb_stats['p90']:.1f}ms "
          f"p95={mdb_stats['p95']:.1f}ms p99={mdb_stats['p99']:.1f}ms")

    # ─── Generate comparison report ───
    print("\n" + "=" * 70)
    print("  COMPARISON REPORT")
    print("=" * 70)

    print(f"\n{'Category':<50s} {'Query':<30s} {'YB(ms)':>10s} {'MDB(ms)':>10s} {'YB/MDB':>8s}")
    print("-" * 112)

    last_cat = ""
    valid_results = []
    for r in results:
        cat = r["category"]
        if cat != last_cat:
            last_cat = cat
        yb_ms = r.get("yb_ms")
        mdb_ms = r.get("mdb_ms")
        yb_str = f"{yb_ms:.1f}" if yb_ms is not None else r.get("yb_error", "ERR")[:9]
        mdb_str = f"{mdb_ms:.1f}" if mdb_ms is not None else r.get("mdb_error", "ERR")[:9]
        ratio = f"{yb_ms/mdb_ms:.2f}x" if (yb_ms and mdb_ms and mdb_ms > 0) else "-"
        print(f"{cat[:50]:<50s} {r['label'][:30]:<30s} {yb_str:>10s} {mdb_str:>10s} {ratio:>8s}")

        if yb_ms is not None and mdb_ms is not None:
            valid_results.append({
                "id": r["id"], "category": cat, "label": r["label"],
                "yb_ms": yb_ms, "mdb_ms": mdb_ms, "ratio": yb_ms / mdb_ms if mdb_ms > 0 else 0,
            })

    # ─── Summary ───
    print(f"\n{'='*70}")
    print(f"  SUMMARY STATISTICS")
    print(f"{'='*70}")

    valid_yb = [r["yb_ms"] for r in valid_results]
    valid_mdb = [r["mdb_ms"] for r in valid_results]
    ratios = [r["ratio"] for r in valid_results]

    yb_s = compute_stats(valid_yb)
    mdb_s = compute_stats(valid_mdb)
    ratio_s = compute_stats(ratios)

    print(f"\n  Valid query pairs: {len(valid_results)}/{len(queries)}")
    print(f"\n  {'Metric':<10s} {'YB(ms)':>10s} {'MDB(ms)':>10s} {'YB/MDB':>8s}")
    print(f"  {'-'*40}")
    for m in ["min", "p50", "p90", "p95", "p99", "max", "mean"]:
        print(f"  {m:<10s} {yb_s[m]:>10.1f} {mdb_s[m]:>10.1f} {yb_s[m]/mdb_s[m] if mdb_s[m] > 0 else 0:>8.2f}x")

    # Save results
    out_dir = os.path.join(SCRIPT_DIR, "bench_results")
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, "all_comparison_ds2000_2ts.json")
    with open(out_path, "w") as f:
        json.dump({
            "dataset": "ds2000",
            "tservers": 2,
            "total_queries": len(queries),
            "valid_pairs": len(valid_results),
            "yb_stats": yb_s,
            "mdb_stats": mdb_s,
            "ratio_stats": ratio_s,
            "results": results,
        }, f, indent=2)
    print(f"\n  Results saved: {out_path}")

if __name__ == "__main__":
    main()
