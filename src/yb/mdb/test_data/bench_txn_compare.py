#!/usr/bin/env python3
"""
Compare transactional vs non-transactional performance:
ds200 dataset, 50 random queries.

Transactions ON:  atomic write + read snapshot consistency
Transactions OFF: original per-triple write + no read snapshot

Usage:
  python3 bench_txn_compare.py
"""

import subprocess
import json
import time
import os
import sys
import statistics
import random

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

YB_CLI = "/home/nonitem/yugabyte-db/build/release-clang21-dynamic-ninja/bin/mdb_cli"
YB_MASTERS = "127.0.0.1:7100"
YB_CTL = "/home/nonitem/yugabyte-db/bin/yb-ctl"

DATASET = sys.argv[1] if len(sys.argv) > 1 else "ds200"
NUM_QUERIES = int(sys.argv[2]) if len(sys.argv) > 2 else 50

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


def ensure_cluster_ready():
    print("Checking YB cluster...")
    out, err, rc = run_cmd(
        f"{YB_CLI} --mdb_masters={YB_MASTERS} --mdb_keyspace=system "
        f"query 'SELECT * WHERE {{ ?s ?p ?o }} LIMIT 1' 2>&1",
        timeout=10)
    if rc == 0:
        print("  Cluster is ready")
        return

    print("  Cluster not responding, starting...")
    run_cmd(f"{YB_CTL} destroy 2>&1", timeout=60, cwd="/home/nonitem/yugabyte-db")
    time.sleep(3)
    out, err, rc = run_cmd(
        f"{YB_CTL} start "
        f"--tserver_flags 'yb_enable_read_committed_isolation=true,cql_proxy_bind_address=127.0.0.1:9042' "
        f"--master_flags 'replication_factor=1' 2>&1",
        timeout=120, cwd="/home/nonitem/yugabyte-db")
    if rc != 0:
        raise RuntimeError(f"yb-ctl start failed: {err or out}")
    print("  Cluster started, waiting for readiness...")
    time.sleep(10)
    for attempt in range(24):
        out, err, rc = run_cmd(
            f"{YB_CLI} --mdb_masters={YB_MASTERS} --mdb_keyspace=system "
            f"query 'SELECT * WHERE {{ ?s ?p ?o }} LIMIT 1' 2>&1",
            timeout=10)
        if rc == 0:
            print(f"  Cluster ready (attempt {attempt+1})")
            return
        time.sleep(5)
    raise RuntimeError("Cluster not ready after 24 attempts")


def import_dataset(keyspace, use_transactions):
    txn_flag = "true" if use_transactions else "false"
    label = "WITH transactions" if use_transactions else "WITHOUT transactions"
    print(f"\nImporting ds200 → keyspace={keyspace} ({label})")
    nt_file = os.path.join(SCRIPT_DIR, f"dataset_{DATASET}.nt")
    t0 = time.time()
    cmd = (f"{YB_CLI} --mdb_masters={YB_MASTERS} --mdb_keyspace={keyspace} "
           f"--mdb_use_transactions={txn_flag} "
           f"import {os.path.abspath(nt_file)} 2>&1")
    out, err, rc = run_cmd(cmd, timeout=3600)
    elapsed = time.time() - t0
    if rc != 0:
        raise RuntimeError(f"Import FAIL ({elapsed:.1f}s): {(err or out)[:500]}")
    print(f"  Import OK ({elapsed:.2f}s)")
    return elapsed


def run_queries(keyspace, queries, use_transactions):
    txn_flag = "true" if use_transactions else "false"
    times_ms = []
    errors = []
    row_counts = []

    for i, qobj in enumerate(queries):
        query = qobj["query"]
        full_query = PREFIXES + "\n" + query
        cmd = (f"{YB_CLI} --mdb_masters={YB_MASTERS} --mdb_keyspace={keyspace} "
               f"--mdb_use_transactions={txn_flag} "
               f"query '{full_query}' 2>&1")
        t0 = time.time()
        out, err, rc = run_cmd(cmd, timeout=600)
        elapsed = time.time() - t0
        elapsed_ms = elapsed * 1000

        if rc != 0:
            errors.append(i)
            print(f"  [{i+1}/{NUM_QUERIES}] ERROR: {(err or out)[:120]}")
        else:
            times_ms.append(elapsed_ms)
            rows = 0
            for line in out.strip().split("\n"):
                if line.strip() == "---":
                    rows += 1
            row_counts.append(rows)

        if (i + 1) % max(1, NUM_QUERIES // 10) == 0:
            pct = 100 * (i + 1) / NUM_QUERIES
            label = f"last={elapsed_ms:.1f}ms" if elapsed_ms else "err"
            print(f"  Progress: {i+1}/{NUM_QUERIES} ({pct:.0f}%)  {label}")

    return times_ms, errors, row_counts


def print_stats(label, stats):
    print(f"\n  --- {label} ---")
    print(f"  Count: {stats['count']}")
    print(f"  Min:   {stats['min']:.2f} ms")
    print(f"  Max:   {stats['max']:.2f} ms")
    print(f"  Mean:  {stats['mean']:.2f} ms")
    print(f"  P50:   {stats['p50']:.2f} ms")
    print(f"  P90:   {stats['p90']:.2f} ms")
    print(f"  P95:   {stats['p95']:.2f} ms")
    print(f"  P99:   {stats['p99']:.2f} ms")


def main():
    random.seed(42)

    pool_file = os.path.join(SCRIPT_DIR, "queries", f"random_{DATASET}.json")
    if not os.path.exists(pool_file):
        print(f"Query pool not found: {pool_file}")
        sys.exit(1)
    with open(pool_file) as f:
        pool = json.load(f)

    if NUM_QUERIES > len(pool):
        queries = random.choices(pool, k=NUM_QUERIES)
    else:
        queries = random.sample(pool, NUM_QUERIES)
    print(f"Loaded {len(pool)} queries from pool, sampling {NUM_QUERIES}")

    ensure_cluster_ready()

    results = {}

    modes = [
        ("txt",  True,  True,  "Write(TXN)+Read(TXN)"),
        ("txx",  True,  False, "Write(TXN)+Read(noTXN)"),
        ("xxt", False, False, "Write(noTXN)+Read(noTXN)"),
    ]

    for label_key, use_txn_write, use_txn_read, desc in modes:
        keyspace = f"bench_{DATASET}_{label_key}"

        write_label = "TXN" if use_txn_write else "noTXN"
        read_label = "TXN" if use_txn_read else "noTXN"
        print(f"\n{'='*60}")
        print(f"  MODE: {desc}")
        print(f"{'='*60}")

        import_time = import_dataset(keyspace, use_txn_write)

        print(f"\nRunning {NUM_QUERIES} queries (read={read_label})...")
        times_ms, errors, row_counts = run_queries(keyspace, queries, use_txn_read)

        stats = compute_stats(times_ms)
        results[label_key] = {
            "desc": desc,
            "write_txn": use_txn_write,
            "read_txn": use_txn_read,
            "import_time_s": import_time,
            "times_ms": times_ms,
            "errors": errors,
            "row_counts": row_counts,
            "stats": stats,
        }

        print_stats(f"db=yb ds={DATASET} q={NUM_QUERIES} {desc}", stats)

    print("\n" + "=" * 72)
    print("  FINAL COMPARISON: 3 modes")
    print("=" * 72)

    headers = ["Metric"] + [results[k]["desc"] for k in ("xxt", "txt", "txx")]
    col_widths = [8, 18, 18, 18]

    hdr = "".join(f"  {h:<{w}}" for h, w in zip(headers, col_widths))
    sep = "".join(f"  {'-'*w}" for w in col_widths)
    print(f"\n{hdr}")
    print(sep)

    for metric in ["mean", "p50", "p90", "p95", "p99", "min", "max"]:
        vals = [results[k]["stats"][metric] for k in ("xxt", "txt", "txx")]
        row = f"  {metric:<{col_widths[0]}}"
        for v in vals:
            row += f"  {v:>{col_widths[1]-2}.2f} ms"
        print(row)

    imp_vals = [results[k]["import_time_s"] for k in ("xxt", "txt", "txx")]
    row = f"  {'import':<{col_widths[0]}}"
    for v in imp_vals:
        row += f"  {v:>{col_widths[1]-2}.2f} s "
    print(row)

    err_vals = [len(results[k]["errors"]) for k in ("xxt", "txt", "txx")]
    row = f"  {'errors':<{col_widths[0]}}"
    for v in err_vals:
        row += f"  {v:>{col_widths[1]-2}}  "
    print(f"\n{row}")

    out_path = os.path.join(SCRIPT_DIR, "bench_results",
                            f"txn_compare_{DATASET}_q{NUM_QUERIES}.json")
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w") as f:
        json.dump({k: {sk: sv for sk, sv in v.items() if sk != "times_ms"}
                   for k, v in results.items()}, f, indent=2)
    print(f"\n  Full results saved to: {out_path}")


if __name__ == "__main__":
    main()
