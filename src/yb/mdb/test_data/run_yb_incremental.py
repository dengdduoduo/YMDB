#!/usr/bin/env python3
"""
YB incremental benchmark: import incremental .nt + run cover queries.
Usage:
  python3 run_yb_incremental.py ds2000 ds4000 2
"""

import subprocess, json, os, sys, time, math, random

random.seed(42)

YB_CLI = "/home/nonitem/yugabyte-db/build/release-clang21-dynamic-ninja/bin/mdb_cli"
YB_MASTERS = "127.0.0.1:7100"
KEYSPACE = "bench_ds2000"


def run_cmd(cmd, timeout=120, cwd="/home/nonitem/yugabyte-db"):
    p = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=timeout, cwd=cwd)
    return p.stdout.strip(), p.stderr.strip(), p.returncode


def run_yb_query(sparql, keyspace, timeout=120):
    escaped = sparql.replace("'", "'\\''")
    out, err, rc = run_cmd(
        f"{YB_CLI} --mdb_masters={YB_MASTERS} --mdb_keyspace={keyspace} "
        f"query '{escaped}' 2>&1",
        timeout=timeout)
    return out, err, rc


def compute_stats(times):
    if not times:
        return {}
    s = sorted(times)
    n = len(s)

    def pct(p):
        idx = p * (n - 1) / 100.0
        lo = int(idx)
        hi = min(lo + 1, n - 1)
        frac = idx - lo
        return s[lo] + frac * (s[hi] - s[lo])

    return {
        "p50": pct(50), "p90": pct(90), "p95": pct(95), "p99": pct(99),
        "min": s[0], "max": s[-1],
        "mean": sum(s) / n,
    }


def main():
    if len(sys.argv) < 4:
        print("Usage: python3 run_yb_incremental.py <base_dataset> <new_dataset> <tservers> [keyspace]")
        sys.exit(1)

    base_name = sys.argv[1]
    new_name = sys.argv[2]
    tservers = int(sys.argv[3])
    if len(sys.argv) >= 5:
        global KEYSPACE
        KEYSPACE = sys.argv[4]

    SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

    # Load queries
    qfile = os.path.join(SCRIPT_DIR, "queries", f"cover_{new_name}.json")
    with open(qfile) as f:
        queries = json.load(f)
    print(f"Loaded {len(queries)} cover queries for {new_name}")

    # Import incremental data
    inc_file = os.path.join(SCRIPT_DIR, f"dataset_{base_name}_to_{new_name}_inc.nt")
    if not os.path.exists(inc_file):
        print(f"ERROR: incremental file not found: {inc_file}")
        sys.exit(1)

    t0 = time.time()
    print(f"Incremental import {inc_file} → keyspace={KEYSPACE}")
    out, err, rc = run_cmd(
        f"{YB_CLI} --mdb_masters={YB_MASTERS} --mdb_keyspace={KEYSPACE} "
        f"import '{inc_file}' 2>&1",
        timeout=300)
    import_time = time.time() - t0
    if rc != 0:
        print(f"Import FAILED: {err[:500]}")
        sys.exit(1)
    print(f"Incremental import OK ({import_time:.1f}s)")

    # Run queries
    times_ms = []
    errors = []
    details = []
    progress_every = 2

    for i, q in enumerate(queries):
        sparql = q["query"]
        t1 = time.time()
        out, err, rc = run_yb_query(sparql, KEYSPACE, timeout=120)
        elapsed = (time.time() - t1) * 1000

        if rc != 0:
            errors.append({"id": q["id"], "template": q["template"], "error": (err or out)[:500]})
        else:
            times_ms.append(elapsed)

        details.append({
            "id": q["id"], "template": q["template"],
            "elapsed_ms": elapsed,
            "error": (err or out)[:500] if rc != 0 else None,
        })

        if (i + 1) % progress_every == 0 or i == len(queries) - 1:
            print(f"  Progress: {i+1}/{len(queries)} ({(i+1)*100//len(queries)}%)  "
                  f"errors={len(errors)}  last={elapsed:.1f}ms")

    stats = compute_stats(times_ms)

    print(f"\n  === RESULTS: yb {new_name} q={len(queries)} ts={tservers} ===")
    print(f"  Total queries: {len(queries)}")
    print(f"  Errors: {len(errors)}")
    print(f"  Valid times: {len(times_ms)}")
    print(f"  p50={stats.get('p50',0):.1f}ms  p90={stats.get('p90',0):.1f}ms  "
          f"p95={stats.get('p95',0):.1f}ms  p99={stats.get('p99',0):.1f}ms")
    print(f"  min={stats.get('min',0):.1f}ms  max={stats.get('max',0):.1f}ms  "
          f"mean={stats.get('mean',0):.1f}ms")

    result = {
        "db": "yb",
        "dataset": new_name,
        "num_queries": len(queries),
        "tservers": tservers,
        "import_time_s": import_time,
        "import_type": "incremental",
        "base_dataset": base_name,
        "times_ms": times_ms,
        "errors": errors,
        "stats": stats,
        "details": details,
    }

    out_dir = os.path.join(SCRIPT_DIR, "bench_results")
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, f"single_yb_{new_name}_q{len(queries)}_ts{tservers}_inc.json")
    with open(out_path, "w") as f:
        json.dump(result, f, indent=2)
    print(f"  Saved: {out_path}")


if __name__ == "__main__":
    main()
