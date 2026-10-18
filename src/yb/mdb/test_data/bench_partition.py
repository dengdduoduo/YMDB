#!/usr/bin/env python3
"""Benchmark script for YB partition loading optimization.

Usage:
  python3 bench_partition.py <mdb_cli> <masters> <keyspace> <cover_json> [options]

Options:
  --runs N        Number of runs (default: 3)
  --warmup N      Warmup runs (default: 1)
  --no-partition  Disable partition loading (for baseline comparison)
  --compare       Run both modes and print comparison
  --json FILE     Write JSON results to file
"""

import json
import subprocess
import sys
import time
import statistics
import os


def parse_cover_queries(json_path):
    with open(json_path) as f:
        queries = json.load(f)
    return [(q["id"], q["template"], q["category"], q["query"]) for q in queries]


def run_query(mdb_cli, masters, keyspace, sparql_query, use_partition, timeout=120):
    env = os.environ.copy()
    env["GLOG_v"] = "0"
    env["GLOG_logtostderr"] = "0"

    args = [mdb_cli,
            f"--mdb_masters={masters}",
            f"--mdb_keyspace={keyspace}"]
    if not use_partition:
        args.append("--mdb_use_partition_load=false")
    args += ["query", sparql_query]

    start = time.monotonic()
    try:
        proc = subprocess.run(args, capture_output=True, text=True,
                              timeout=timeout, env=env)
        elapsed = time.monotonic() - start
    except subprocess.TimeoutExpired:
        return None, timeout, "TIMEOUT"
    except Exception as e:
        return None, time.monotonic() - start, str(e)

    if proc.returncode != 0:
        return None, elapsed, f"EXIT={proc.returncode}; stderr={proc.stderr[:200]}"

    lines = [l.strip() for l in proc.stdout.strip().split("\n")
             if l.strip() and l.strip() != "---"]
    result_count = len(lines)
    return result_count, elapsed, None


def run_benchmark(mdb_cli, masters, keyspace, queries,
                  use_partition=True, runs=3, warmup=1):
    total_queries = len(queries)
    mode = "PARTITION" if use_partition else "BASELINE"

    print(f"\n{'=' * 70}")
    print(f"MODE: {mode}  |  {total_queries} queries x {runs} runs (warmup={warmup})")
    print(f"Target: {keyspace} @ {masters}")
    print(f"{'=' * 70}")

    all_latencies = []

    for run_idx in range(runs + warmup):
        is_warmup = run_idx < warmup
        label = f"WARMUP {run_idx + 1}" if is_warmup else f"RUN {run_idx - warmup + 1}"
        run_latencies = []

        print(f"\n--- {label} ---")
        for qid, template, category, sparql in queries:
            nrows, elapsed, error = run_query(
                mdb_cli, masters, keyspace, sparql, use_partition)
            status = "OK" if error is None else f"ERR: {error}"
            ms = elapsed * 1000
            if not is_warmup:
                run_latencies.append(ms)
                all_latencies.append(ms)
            nrows_str = str(nrows) if nrows is not None else "ERR"
            print(f"  Q{qid:02d} [{template:25s}] {nrows_str:>5s} rows  {ms:8.1f}ms  {status}")

        if not is_warmup:
            sorted_lat = sorted(run_latencies)
            p50_idx = max(int(len(sorted_lat) * 0.5) - 1, 0)
            p95_idx = max(int(len(sorted_lat) * 0.95) - 1, 0)
            p99_idx = max(int(len(sorted_lat) * 0.99) - 1, 0)
            print(f"  Summary: P50={sorted_lat[p50_idx]:.1f}ms  "
                  f"P95={sorted_lat[p95_idx]:.1f}ms  "
                  f"P99={sorted_lat[p99_idx]:.1f}ms  "
                  f"AVG={statistics.mean(run_latencies):.1f}ms")

    sorted_all = sorted(all_latencies)
    n = len(sorted_all)
    if n == 0:
        return {"mode": mode, "p50": 0, "p95": 0, "p99": 0, "avg": 0, "min": 0, "max": 0,
                "samples": []}

    p50_idx = max(int(n * 0.5) - 1, 0)
    p95_idx = max(int(n * 0.95) - 1, 0)
    p99_idx = max(int(n * 0.99) - 1, 0)
    result = {
        "mode": mode,
        "p50": sorted_all[p50_idx],
        "p95": sorted_all[p95_idx],
        "p99": sorted_all[p99_idx],
        "avg": statistics.mean(sorted_all),
        "min": min(sorted_all),
        "max": max(sorted_all),
        "samples": sorted_all,
    }

    print(f"\nFINAL {mode}:")
    print(f"  P50={result['p50']:.1f}ms  P95={result['p95']:.1f}ms  "
          f"P99={result['p99']:.1f}ms  AVG={result['avg']:.1f}ms")
    print(f"  MIN={result['min']:.1f}ms  MAX={result['max']:.1f}ms")
    return result


def print_comparison(part, base):
    print(f"\n{'=' * 70}")
    print("COMPARISON: PARTITION vs BASELINE")
    print(f"{'=' * 70}")
    print(f"{'Metric':<8} {'BASELINE':>12} {'PARTITION':>12} {'SPEEDUP':>10}")
    print(f"{'-' * 42}")
    for metric in ["p50", "p95", "p99", "avg", "min", "max"]:
        b = base[metric]
        p = part[metric]
        if b > 0:
            ratio = b / p
        else:
            ratio = float('inf')
        print(f"{metric:<8} {b:>10.1f}ms {p:>10.1f}ms {ratio:>8.2f}x")


def main():
    if len(sys.argv) < 5:
        print(__doc__)
        sys.exit(1)

    mdb_cli = sys.argv[1]
    masters = sys.argv[2]
    keyspace = sys.argv[3]
    cover_json = sys.argv[4]

    runs = 3
    warmup = 1
    use_partition = True
    do_compare = False
    json_file = None

    i = 5
    while i < len(sys.argv):
        if sys.argv[i] == "--runs" and i + 1 < len(sys.argv):
            runs = int(sys.argv[i + 1])
            i += 2
        elif sys.argv[i] == "--warmup" and i + 1 < len(sys.argv):
            warmup = int(sys.argv[i + 1])
            i += 2
        elif sys.argv[i] == "--no-partition":
            use_partition = False
            i += 1
        elif sys.argv[i] == "--compare":
            do_compare = True
            i += 1
        elif sys.argv[i] == "--json" and i + 1 < len(sys.argv):
            json_file = sys.argv[i + 1]
            i += 2
        else:
            i += 1

    if not os.path.exists(mdb_cli):
        print(f"ERROR: mdb_cli not found at {mdb_cli}")
        sys.exit(1)

    queries = parse_cover_queries(cover_json)
    print(f"Loaded {len(queries)} queries from {cover_json}")

    t0 = time.monotonic()

    if do_compare:
        base_result = run_benchmark(
            mdb_cli, masters, keyspace, queries,
            use_partition=False, runs=runs, warmup=warmup)
        part_result = run_benchmark(
            mdb_cli, masters, keyspace, queries,
            use_partition=True, runs=runs, warmup=warmup)
        print_comparison(part_result, base_result)

        if json_file:
            with open(json_file, "w") as f:
                json.dump({"baseline": base_result, "partition": part_result}, f, indent=2)
            print(f"\nResults saved to {json_file}")
    else:
        result = run_benchmark(
            mdb_cli, masters, keyspace, queries,
            use_partition=use_partition, runs=runs, warmup=warmup)

        if json_file:
            with open(json_file, "w") as f:
                json.dump(result, f, indent=2)
            print(f"\nResults saved to {json_file}")

    total = time.monotonic() - t0
    print(f"\nTotal wall time: {total:.1f}s")


if __name__ == "__main__":
    main()
