#!/usr/bin/env python3
"""
Random SPARQL Benchmark Runner — compares YugabyteDB MDB vs MillenniumDB
using random queries at different counts (10/50/100/200/500/1000/2000).

Workflow:
  1. Load random query pool (already generated)
  2. Import dataset to YB and MillenniumDB
  3. For each query count, sample & run queries
  4. Record p50/p90/p95/p99/max and correctness
  5. Save results per count

Usage:
  python3 run_random_benchmark.py <dataset_name> <pool_size>
  python3 run_random_benchmark.py ds100 2000
"""

import subprocess
import json
import time
import os
import sys
import statistics
import signal
import random

YB_CLI = "/home/nonitem/yugabyte-db/build/release-clang21-dynamic-ninja/bin/mdb_cli"
YB_MASTERS = "127.0.0.1:7100"
YB_KEYSPACE_PREFIX = "bench_rnd"

MDB_BIN = "/home/nonitem/MillenniumDB/build/bin/mdb"
MDB_PORT = 9501

QUERY_COUNTS = [10, 50, 100, 200, 500, 1000, 2000]

PREFIXES = """PREFIX ex: <http://example.org/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
"""


def run_cmd(cmd, timeout=120):
    try:
        result = subprocess.run(
            cmd, shell=True, capture_output=True, text=True, timeout=timeout
        )
        return result.stdout, result.stderr, result.returncode
    except subprocess.TimeoutExpired:
        return "", "TIMEOUT", -1


def check_yb_cluster():
    stdout, stderr, rc = run_cmd("ps aux | grep yb-master | grep -v grep", timeout=5)
    if rc != 0:
        raise RuntimeError("YB master not running!")


def yb_import(dataset_name, nt_file):
    keyspace = f"{YB_KEYSPACE_PREFIX}_{dataset_name}"
    print(f"  Importing {nt_file} into YB keyspace={keyspace}...")
    t0 = time.time()
    stdout, stderr, rc = run_cmd(
        f"{YB_CLI} --mdb_masters={YB_MASTERS} --mdb_keyspace={keyspace} "
        f'import {os.path.abspath(nt_file)} 2>&1',
        timeout=600,
    )
    elapsed = time.time() - t0
    if rc != 0:
        stderr_short = (stderr or "")[:500]
        raise RuntimeError(f"YB import failed ({elapsed:.1f}s): {stderr_short}")
    print(f"  YB import OK ({elapsed:.1f}s)")
    return keyspace


def yb_query(keyspace, sparql_query):
    full_query = PREFIXES + "\n" + sparql_query
    t0 = time.time()
    stdout, stderr, rc = run_cmd(
        f"{YB_CLI} --mdb_masters={YB_MASTERS} --mdb_keyspace={keyspace} "
        f"query '{full_query}' 2>&1",
        timeout=120,
    )
    elapsed = time.time() - t0

    if rc != 0:
        return None, elapsed, (stderr or stdout)[:500]

    rows = []
    current_row = []
    for line in stdout.strip().split("\n"):
        line = line.strip()
        if line == "---":
            if current_row:
                rows.append(tuple(current_row))
                current_row = []
            continue
        if "\t" in line:
            parts = line.split("\t", 1)
            if len(parts) == 2:
                current_row.append(parts[1])
    if current_row:
        rows.append(tuple(current_row))

    return rows, elapsed, None


class MdbServer:
    def __init__(self, db_folder, port):
        self.db_folder = db_folder
        self.port = port
        self.process = None

    def start(self):
        cmd = f"{MDB_BIN} server {self.db_folder} --port {self.port} --browser false 2>&1"
        self.process = subprocess.Popen(
            cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
        )
        time.sleep(2)
        poll = self.process.poll()
        if poll is not None:
            output = self.process.stdout.read().decode() if self.process.stdout else ""
            raise RuntimeError(f"MDB server exited early (code={poll}): {output[:500]}")
        print(f"  MDB server started on port {self.port}")

    def stop(self):
        if self.process:
            self.process.send_signal(signal.SIGTERM)
            try:
                self.process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait()
            self.process = None

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, *args):
        self.stop()


def mdb_import(nt_file, db_folder):
    print(f"  Importing {nt_file} into MillenniumDB {db_folder}...")
    t0 = time.time()
    stdout, stderr, rc = run_cmd(
        f"{MDB_BIN} import {os.path.abspath(nt_file)} {db_folder} --format nt 2>&1",
        timeout=600,
    )
    elapsed = time.time() - t0
    if rc != 0:
        stderr_short = (stderr or stdout)[:500]
        raise RuntimeError(f"MDB import failed ({elapsed:.1f}s): {stderr_short}")
    print(f"  MDB import OK ({elapsed:.1f}s)")
    return elapsed


def mdb_query(sparql_query, port):
    full_query = PREFIXES + "\n" + sparql_query
    t0 = time.time()
    stdout, stderr, rc = run_cmd(
        f'curl -s -X POST http://localhost:{port}/sparql '
        f'-H "Content-Type: application/sparql-query" '
        f"--data '{full_query}' 2>&1",
        timeout=120,
    )
    elapsed = time.time() - t0

    if rc != 0 or not stdout.strip():
        return None, elapsed, (stderr or stdout or "empty response")[:500]

    try:
        data = json.loads(stdout)
        rows = []
        for binding in data.get("results", {}).get("bindings", []):
            row = tuple(binding.get(v, {}).get("value", "") for v in data["head"]["vars"])
            rows.append(row)
        return rows, elapsed, None
    except json.JSONDecodeError:
        return None, elapsed, stdout[:500]


def compute_stats(times):
    if not times:
        return {"count": 0, "min": 0, "max": 0, "mean": 0,
                "p50": 0, "p90": 0, "p95": 0, "p99": 0}
    times_sorted = sorted(times)
    n = len(times_sorted)

    def percentile(p):
        k = (n - 1) * p
        f = int(k)
        c = k - f
        if f + 1 < n:
            return times_sorted[f] + c * (times_sorted[f + 1] - times_sorted[f])
        return times_sorted[f]

    return {
        "count": n,
        "min": min(times_sorted),
        "max": max(times_sorted),
        "mean": statistics.mean(times_sorted),
        "p50": percentile(0.50),
        "p90": percentile(0.90),
        "p95": percentile(0.95),
        "p99": percentile(0.99),
    }


def run_random_benchmark(dataset_name, pool_size):
    print(f"\n{'='*60}")
    print(f"Random Benchmark: {dataset_name} (pool={pool_size})")
    print(f"{'='*60}")

    nt_file = os.path.join(os.path.dirname(__file__), f"dataset_{dataset_name}.nt")
    if not os.path.exists(nt_file):
        raise FileNotFoundError(f"Dataset not found: {nt_file}")

    queries_file = os.path.join(
        os.path.dirname(__file__), "queries", f"random_{dataset_name}.json"
    )
    if not os.path.exists(queries_file):
        print(f"  Generating random query pool ({pool_size})...")
        subprocess.run(
            [sys.executable, os.path.join(os.path.dirname(__file__),
             "generate_random_queries.py"), dataset_name, str(pool_size)],
            check=True,
        )

    with open(queries_file) as f:
        all_queries = json.load(f)
    print(f"  Loaded {len(all_queries)} random queries from pool")

    results = {
        "dataset": dataset_name,
        "pool_size": len(all_queries),
        "query_counts": [],
    }

    # Import data
    check_yb_cluster()
    keyspace = yb_import(dataset_name, nt_file)

    db_folder = f"/tmp/mdb_rnd_{dataset_name}"
    mdb_import(nt_file, db_folder)
    server = MdbServer(db_folder, MDB_PORT)
    server.start()

    try:
        random.seed(42)

        for num_queries in QUERY_COUNTS:
            if num_queries > len(all_queries):
                print(f"  Count={num_queries} > pool ({len(all_queries)}), using sampling with replacement")

            print(f"\n  --- Query count: {num_queries} ---")

            if num_queries <= len(all_queries):
                sample = random.sample(all_queries, num_queries)
            else:
                sample = random.choices(all_queries, k=num_queries)

            count_result = {
                "num_queries": num_queries,
                "yb_times_ms": [],
                "mdb_times_ms": [],
                "yb_errors": 0,
                "mdb_errors": 0,
                "correct": 0,
                "wrong": 0,
                "both_errors": 0,
                "details": [],
            }

            for i, qobj in enumerate(sample):
                if num_queries <= 100 or i % max(1, num_queries // 20) == 0:
                    print(f"    Q {i+1}/{num_queries} ({qobj['template']})")

                query = qobj["query"]

                yb_rows, yb_time, yb_err = yb_query(keyspace, query)
                yb_time_ms = yb_time * 1000

                mdb_rows, mdb_time, mdb_err = mdb_query(query, MDB_PORT)
                mdb_time_ms = mdb_time * 1000

                detail = {
                    "id": qobj["id"],
                    "template": qobj["template"],
                    "category": qobj["category"],
                    "yb_time_ms": yb_time_ms,
                    "mdb_time_ms": mdb_time_ms,
                }

                if yb_err:
                    detail["yb_error"] = yb_err[:200]
                    count_result["yb_errors"] += 1
                else:
                    count_result["yb_times_ms"].append(yb_time_ms)

                if mdb_err:
                    detail["mdb_error"] = mdb_err[:200]
                    count_result["mdb_errors"] += 1
                else:
                    count_result["mdb_times_ms"].append(mdb_time_ms)

                if yb_err and mdb_err:
                    count_result["both_errors"] += 1
                elif yb_err or mdb_err:
                    count_result["wrong"] += 1
                else:
                    yb_norm = sorted(yb_rows) if yb_rows else []
                    mdb_norm = sorted(mdb_rows) if mdb_rows else []
                    if len(yb_norm) == len(mdb_norm):
                        count_result["correct"] += 1
                    else:
                        count_result["wrong"] += 1
                        detail["yb_rowcount"] = len(yb_norm)
                        detail["mdb_rowcount"] = len(mdb_norm)

                count_result["details"].append(detail)

            yb_stats = compute_stats(count_result["yb_times_ms"])
            mdb_stats = compute_stats(count_result["mdb_times_ms"])
            total = num_queries
            acc = 100 * count_result["correct"] / total if total > 0 else 0

            print(f"    YB: p50={yb_stats['p50']:.1f}ms p90={yb_stats['p90']:.1f}ms "
                  f"p95={yb_stats['p95']:.1f}ms p99={yb_stats['p99']:.1f}ms max={yb_stats['max']:.1f}ms "
                  f"mean={yb_stats['mean']:.1f}ms err={count_result['yb_errors']}")
            print(f"    MDB: p50={mdb_stats['p50']:.1f}ms p90={mdb_stats['p90']:.1f}ms "
                  f"p95={mdb_stats['p95']:.1f}ms p99={mdb_stats['p99']:.1f}ms max={mdb_stats['max']:.1f}ms "
                  f"mean={mdb_stats['mean']:.1f}ms err={count_result['mdb_errors']}")
            print(f"    Accuracy: {count_result['correct']}/{total} = {acc:.1f}%")

            count_result["yb_stats"] = yb_stats
            count_result["mdb_stats"] = mdb_stats
            count_result["accuracy"] = acc
            results["query_counts"].append(count_result)

    finally:
        server.stop()

    return results


def main():
    if len(sys.argv) < 3:
        print("Usage: run_random_benchmark.py <dataset_name> <pool_size>")
        print("  dataset_name: ds100, ds200, ds500, ds1000, ds2000")
        print("  pool_size: e.g. 2000")
        sys.exit(1)

    dataset_name = sys.argv[1]
    pool_size = int(sys.argv[2])

    results = run_random_benchmark(dataset_name, pool_size)

    out_dir = os.path.join(os.path.dirname(__file__), "bench_results")
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, f"random_result_{dataset_name}.json")
    with open(out_path, "w") as f:
        json.dump(results, f, indent=2, default=str)
    print(f"\n  Results saved to {out_path}")


if __name__ == "__main__":
    main()
