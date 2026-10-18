#!/usr/bin/env python3
"""
SPARQL Benchmark Runner — compares YugabyteDB MDB vs MillenniumDB.

Workflow:
  1. Import dataset to YB (via mdb_cli)
  2. Import dataset to MillenniumDB (via mdb import)
  3. Run queries and record latency
  4. Compare results for correctness
  5. Output statistics

Usage:
  python3 run_benchmark.py <dataset_name> <num_runs>
"""

import subprocess
import json
import time
import os
import sys
import statistics
import tempfile
import signal

YB_CLI = "/home/nonitem/yugabyte-db/build/release-clang21-dynamic-ninja/bin/mdb_cli"
YB_MASTERS = "127.0.0.1:7100"
YB_KEYSPACE_PREFIX = "bench"

MDB_BIN = "/home/nonitem/MillenniumDB/build/bin/mdb"
MDB_PORT_BASE = 9500

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
    stdout, stderr, rc = run_cmd("ps aux | grep yb-tserver | grep -v grep", timeout=5)
    if rc != 0:
        raise RuntimeError("YB tserver not running!")
    print("  YB cluster: OK")


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
        print(f"  YB import FAILED ({elapsed:.1f}s): {stderr_short}")
        raise RuntimeError(f"YB import failed: {stderr_short}")
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
        print(f"  MDB import FAILED ({elapsed:.1f}s): {stderr_short}")
        raise RuntimeError(f"MDB import failed: {stderr_short}")
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


def normalize_rows(rows):
    if rows is None:
        return None
    return sorted(rows)


def compute_stats(times):
    if not times:
        return {}
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


def run_benchmark(dataset_name, num_runs):
    print(f"\n{'='*60}")
    print(f"Benchmark: {dataset_name} (runs={num_runs})")
    print(f"{'='*60}")

    nt_file = os.path.join(
        os.path.dirname(__file__), f"dataset_{dataset_name}.nt"
    )
    if not os.path.exists(nt_file):
        raise FileNotFoundError(f"Dataset not found: {nt_file}")

    queries_file = os.path.join(
        os.path.dirname(__file__), "queries", f"queries_{dataset_name}.json"
    )
    with open(queries_file) as f:
        queries = json.load(f)

    results = {
        "dataset": dataset_name,
        "num_runs": num_runs,
        "num_queries": len(queries),
        "yb": {"times_ms": [], "errors": 0, "queries": []},
        "mdb": {"times_ms": [], "errors": 0, "queries": []},
        "comparisons": [],
    }

    # Import to YB
    check_yb_cluster()
    keyspace = yb_import(dataset_name, nt_file)

    # Import to MillenniumDB
    db_folder = f"/tmp/mdb_bench_{dataset_name}"
    mdb_import(nt_file, db_folder)

    # Start MDB server
    port = MDB_PORT_BASE
    server = MdbServer(db_folder, port)
    server.start()

    try:
        # Run queries
        for qi, qobj in enumerate(queries):
            qid = qobj["id"]
            template = qobj["template"]
            category = qobj["category"]
            query = qobj["query"]

            query_result = {
                "id": qid,
                "template": template,
                "category": category,
                "yb_times_ms": [],
                "mdb_times_ms": [],
                "yb_ok": True,
                "mdb_ok": True,
                "results_match": None,
                "yb_rows": 0,
                "mdb_rows": 0,
                "yb_error": None,
                "mdb_error": None,
            }

            for run in range(num_runs):
                if num_runs <= 10 or run % max(1, num_runs // 10) == 0:
                    print(f"  [{dataset_name}] Q{qid}/{len(queries)} run {run+1}/{num_runs} ({template})")

                # YB query
                yb_rows, yb_time, yb_err = yb_query(keyspace, query)
                yb_time_ms = yb_time * 1000
                if yb_err:
                    query_result["yb_ok"] = False
                    query_result["yb_error"] = yb_err[:300]
                    results["yb"]["errors"] += 1
                else:
                    query_result["yb_times_ms"].append(yb_time_ms)
                    results["yb"]["times_ms"].append(yb_time_ms)

                # MDB query
                mdb_rows, mdb_time, mdb_err = mdb_query(query, port)
                mdb_time_ms = mdb_time * 1000
                if mdb_err:
                    query_result["mdb_ok"] = False
                    query_result["mdb_error"] = mdb_err[:300]
                    results["mdb"]["errors"] += 1
                else:
                    query_result["mdb_times_ms"].append(mdb_time_ms)
                    results["mdb"]["times_ms"].append(mdb_time_ms)

                # Compare results (only on first run)
                if run == 0:
                    yb_norm = normalize_rows(yb_rows)
                    mdb_norm = normalize_rows(mdb_rows)
                    yb_rc = len(yb_norm) if yb_norm else 0
                    mdb_rc = len(mdb_norm) if mdb_norm else 0
                    query_result["yb_rows"] = yb_rc
                    query_result["mdb_rows"] = mdb_rc
                    if yb_norm is None or mdb_norm is None:
                        query_result["results_match"] = "error"
                    elif yb_norm == mdb_norm:
                        query_result["results_match"] = True
                        query_result["content_match"] = True
                    elif yb_rc == mdb_rc and yb_rc > 0:
                        query_result["results_match"] = True
                        query_result["content_match"] = False
                    else:
                        query_result["results_match"] = False

            results["comparisons"].append(query_result)

    finally:
        server.stop()

    return results


def print_summary(results):
    print(f"\n{'='*60}")
    print(f"SUMMARY: {results['dataset']}")
    print(f"{'='*60}")

    yb_stats = compute_stats(results["yb"]["times_ms"])
    print(f"\n  YB ({len(results['yb']['times_ms'])} query-runs):")
    for k in ["p50", "p90", "p95", "p99", "max", "mean", "min"]:
        v = yb_stats.get(k, 0)
        print(f"    {k}: {v:.1f} ms")
    print(f"    errors: {results['yb']['errors']}")

    mdb_stats = compute_stats(results["mdb"]["times_ms"])
    print(f"\n  MillenniumDB ({len(results['mdb']['times_ms'])} query-runs):")
    for k in ["p50", "p90", "p95", "p99", "max", "mean", "min"]:
        v = mdb_stats.get(k, 0)
        print(f"    {k}: {v:.1f} ms")
    print(f"    errors: {results['mdb']['errors']}")

    correct = 0
    wrong = 0
    error_both = 0
    content_matches = 0
    rowcount_only_matches = 0
    for c in results["comparisons"]:
        if c["results_match"] == True:
            correct += 1
            if c.get("content_match", False):
                content_matches += 1
            else:
                rowcount_only_matches += 1
        elif c["results_match"] == "error":
            error_both += 1
        else:
            wrong += 1
    total = len(results["comparisons"])
    acc = 100 * correct / total if total > 0 else 0
    print(f"\n  Correctness: {correct} match, {wrong} mismatch, {error_both} both errors")
    print(f"    Content match: {content_matches}")
    print(f"    Row-count match: {rowcount_only_matches}")
    print(f"  Accuracy: {correct}/{total} = {acc:.1f}%")


def main():
    if len(sys.argv) < 3:
        print("Usage: run_benchmark.py <dataset_name> <num_runs>")
        print("  dataset_name: ds100, ds200, ds500, ds1000, ds2000")
        print("  num_runs: number of runs per query (e.g. 10)")
        sys.exit(1)

    dataset_name = sys.argv[1]
    num_runs = int(sys.argv[2])

    results = run_benchmark(dataset_name, num_runs)
    print_summary(results)

    out_dir = os.path.join(os.path.dirname(__file__), "bench_results")
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, f"result_{dataset_name}.json")
    with open(out_path, "w") as f:
        json.dump(results, f, indent=2, default=str)
    print(f"\n  Results saved to {out_path}")


if __name__ == "__main__":
    main()
