#!/usr/bin/env python3

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from testing import options
from testing.file_handler import get_official_tests, get_internal_tests
from testing.cluster_utils import verify_cluster_alive, import_data
from testing.logging import Level, log, print_summary
from testing.query_execution import execute_tests
from testing.types import ExecutionStats


def run_yb_tests(suites_name: str = "official"):
    if not options.MDB_CLI_PATH.exists():
        log(Level.ERROR, f"mdb_cli not found at {options.MDB_CLI_PATH}")
        sys.exit(1)

    log(Level.BEGIN, "Verifying YB cluster is alive...")
    if not verify_cluster_alive():
        log(Level.ERROR, "YB cluster not reachable. Start it with yb-ctl first.")
        sys.exit(1)
    log(Level.CORRECT, "YB cluster reachable")

    if suites_name == "official":
        suites = get_official_tests()
    elif suites_name == "internal":
        suites = get_internal_tests()
    else:
        suites = get_official_tests() + get_internal_tests()

    if not suites:
        log(Level.ERROR, "No test suites found")
        sys.exit(1)

    total_tests = sum(len(suite.tests) for suite in suites)
    log(Level.BEGIN, f"Running {len(suites)} suites ({total_tests} total tests)")

    all_stats = execute_tests(suites)

    log(Level.SUMMARY, "")
    log(Level.SUMMARY, "=" * 60)
    log(Level.SUMMARY, "  W3C SPARQL 1.1 Test Suite Results (YugabyteDB)")
    log(Level.SUMMARY, "=" * 60)

    total_stats = ExecutionStats(name="YB TOTAL")
    for stats in all_stats:
        total_stats += stats

    for stats in all_stats:
        stats.log()

    total_stats.name = "YB TOTAL"
    total_stats.log()
    print_summary()

    return total_stats


def main():
    parser = argparse.ArgumentParser(description="W3C SPARQL 1.1 Test Suite Runner for YugabyteDB")
    parser.add_argument("--masters", default=options.MDB_MASTERS,
                        help="YB master addresses (default: 127.0.0.1:7100)")
    parser.add_argument("--cli", default=str(options.MDB_CLI_PATH),
                        help="Path to mdb_cli binary")
    parser.add_argument("--suites", choices=["official", "internal", "all"], default="official",
                        help="Which test suites to run (default: official)")
    parser.add_argument("--timeout", type=int, default=options.TIMEOUT,
                        help=f"Query timeout in seconds (default: {options.TIMEOUT})")
    parser.add_argument("--keyspace-prefix", default=options.MDB_KEYSPACE_PREFIX,
                        help=f"Keyspace prefix for W3C tests (default: {options.MDB_KEYSPACE_PREFIX})")
    args = parser.parse_args()

    options.MDB_MASTERS = args.masters
    options.MDB_CLI_PATH = Path(args.cli)
    options.TIMEOUT = args.timeout
    options.MDB_KEYSPACE_PREFIX = args.keyspace_prefix

    stats = run_yb_tests(args.suites)

    total = stats.total
    passed = stats.correct
    pct = (passed / total * 100) if total > 0 else 0
    log(Level.SUMMARY, f"Pass rate: {passed}/{total} = {pct:.1f}%")
    print_summary()

    if stats.error > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
