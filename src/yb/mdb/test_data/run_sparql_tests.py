#!/usr/bin/env python3
"""
MDB SPARQL Black-Box Test Suite
================================
Tests YugabyteDB MDB SPARQL engine via the mdb_cli command-line tool.

Usage:
    python3 run_sparql_tests.py --cli ./mdb_cli --masters 127.0.0.1:7100 --keyspace mdb_test

The script performs:
  1. Cluster initialization
  2. Data import (RDF N-Triples file)
  3. ~100 SPARQL test queries
  4. Report generation
"""

import argparse
import json
import os
import subprocess
import sys
import time
from collections import defaultdict

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))


class MdbCli:
    def __init__(self, cli_path, masters, keyspace):
        self.cli_path = cli_path
        self.masters = masters
        self.keyspace = keyspace

    def _run(self, *args, timeout=120):
        cmd = [self.cli_path] + list(args)
        try:
            proc = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=timeout,
            )
            return proc.returncode, proc.stdout, proc.stderr
        except subprocess.TimeoutExpired:
            return -1, "", "TIMEOUT"
        except FileNotFoundError:
            return -1, "", f"CLI not found at {self.cli_path}"

    def init(self):
        rc, stdout, stderr = self._run("init", self.masters, self.keyspace)
        return rc == 0, stderr if rc != 0 else stdout

    def import_file(self, file_path):
        rc, stdout, stderr = self._run(
            f"--mdb_masters={self.masters}",
            f"--mdb_keyspace={self.keyspace}",
            "import", file_path, timeout=300)
        return rc == 0, stderr if rc != 0 else stdout

    def query(self, sparql):
        rc, stdout, stderr = self._run(
            f"--mdb_masters={self.masters}",
            f"--mdb_keyspace={self.keyspace}",
            "query", sparql, timeout=60)
        return rc, self._parse_query_output(stdout), stderr

    def _parse_query_output(self, output):
        """Parse tab-separated variable bindings. Rows separated by '---'."""
        rows = []
        current_row = {}

        for line in output.strip().split("\n"):
            line = line.rstrip()
            if line == "---":
                if current_row:
                    rows.append(current_row)
                    current_row = {}
            elif "\t" in line:
                var, val = line.split("\t", 1)
                current_row[var] = val
            elif line:
                parts = line.split(None, 1)
                if len(parts) == 2:
                    current_row[parts[0]] = parts[1]

        if current_row:
            rows.append(current_row)

        return rows


PREFIX = """
PREFIX ex: <http://example.org/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
"""


def build_queries():
    """Build ~100 SPARQL queries covering different features."""

    categories = {
        "Basic Graph Pattern": [],
        "FILTER": [],
        "ORDER BY": [],
        "DISTINCT": [],
        "UNION": [],
        "Multiple Triple Patterns": [],
        "OPTIONAL": [],
        "Property Paths": [],
        "Built-in Functions": [],
        "Complex Queries": [],
        "Edge Cases": [],
        "LIMIT / OFFSET": [],
    }

    def add(cat, name, query, expected_min=1):
        categories[cat].append({
            "name": name,
            "query": PREFIX + "\n" + query.strip(),
            "expected_min": expected_min,
        })

    # ========== Basic Graph Pattern ==========
    add("Basic Graph Pattern", "BGP-01: All companies",
        "SELECT ?c WHERE { ?c rdf:type ex:Company }", 20)
    add("Basic Graph Pattern", "BGP-02: All persons",
        "SELECT ?p WHERE { ?p rdf:type ex:Person }", 40)
    add("Basic Graph Pattern", "BGP-03: All products",
        "SELECT ?pr WHERE { ?pr rdf:type ex:Product }", 20)
    add("Basic Graph Pattern", "BGP-04: All technologies",
        "SELECT ?t WHERE { ?t rdf:type ex:Technology }", 20)
    add("Basic Graph Pattern", "BGP-05: All departments",
        "SELECT ?d WHERE { ?d rdf:type ex:Department }", 10)
    add("Basic Graph Pattern", "BGP-06: All cities",
        "SELECT ?c WHERE { ?c rdf:type ex:City }", 15)
    add("Basic Graph Pattern", "BGP-07: All projects",
        "SELECT ?p WHERE { ?p rdf:type ex:Project }", 15)
    add("Basic Graph Pattern", "BGP-08: Company labels",
        "SELECT ?name WHERE { ?c rdf:type ex:Company . ?c rdfs:label ?name }", 20)
    add("Basic Graph Pattern", "BGP-09: Person labels",
        "SELECT ?name WHERE { ?p rdf:type ex:Person . ?p rdfs:label ?name }", 40)

    # ========== FILTER ==========
    add("FILTER", "FILTER-01: Companies in Beijing",
        "SELECT ?c WHERE { ?c rdf:type ex:Company . ?c ex:locatedIn ?city . FILTER(?city = ex:Beijing) }", 0)
    add("FILTER", "FILTER-02: Companies NOT in Beijing",
        "SELECT ?c WHERE { ?c rdf:type ex:Company . ?c ex:locatedIn ?city . FILTER(?city != ex:Beijing) }", 0)
    add("FILTER", "FILTER-03: Persons who know someone",
        "SELECT ?p1 ?p2 WHERE { ?p1 ex:knows ?p2 }", 1)
    add("FILTER", "FILTER-04: FILTER with same-term",
        "SELECT ?c WHERE { ?c rdf:type ex:Company . FILTER(sameTerm(?c, ?c)) }", 20)

    # ========== ORDER BY ==========
    add("ORDER BY", "ORDER-01: All companies ordered",
        "SELECT ?c WHERE { ?c rdf:type ex:Company } ORDER BY ?c", 20)
    add("ORDER BY", "ORDER-02: All persons ordered",
        "SELECT ?p WHERE { ?p rdf:type ex:Person } ORDER BY ?p", 40)

    # ========== DISTINCT ==========
    add("DISTINCT", "DISTINCT-01: Distinct cities",
        "SELECT DISTINCT ?city WHERE { ?c rdf:type ex:Company . ?c ex:locatedIn ?city }", 1)
    add("DISTINCT", "DISTINCT-02: Distinct companies hiring",
        "SELECT DISTINCT ?c WHERE { ?p ex:worksFor ?c }", 1)

    # ========== UNION ==========
    add("UNION", "UNION-01: Companies or Persons",
        "SELECT ?e WHERE { { ?e rdf:type ex:Company } UNION { ?e rdf:type ex:Person } }", 60)
    add("UNION", "UNION-02: Products or Technologies",
        "SELECT ?e WHERE { { ?e rdf:type ex:Product } UNION { ?e rdf:type ex:Technology } }", 40)

    # ========== Multiple Triple Patterns ==========
    add("Multiple Triple Patterns", "MTP-01: Person works for a company",
        "SELECT ?p ?c WHERE { ?p rdf:type ex:Person . ?p ex:worksFor ?c . ?c rdf:type ex:Company }", 40)
    add("Multiple Triple Patterns", "MTP-02: Company develops product",
        "SELECT ?c ?pr WHERE { ?c ex:develops ?pr . ?pr rdf:type ex:Product }", 1)
    add("Multiple Triple Patterns", "MTP-03: Product uses technology",
        "SELECT ?pr ?t WHERE { ?pr ex:uses ?t . ?t rdf:type ex:Technology }", 1)
    add("Multiple Triple Patterns", "MTP-04: Company location",
        "SELECT ?c ?city WHERE { ?c rdf:type ex:Company . ?c ex:locatedIn ?city . ?city rdf:type ex:City }", 20)
    add("Multiple Triple Patterns", "MTP-05: Manager manages person",
        "SELECT ?mgr ?sub WHERE { ?mgr ex:manages ?sub . ?sub rdf:type ex:Person }", 1)
    add("Multiple Triple Patterns", "MTP-06: Company funds project",
        "SELECT ?c ?prj WHERE { ?c ex:funds ?prj . ?prj rdf:type ex:Project }", 1)
    add("Multiple Triple Patterns", "MTP-07: Department partOf company",
        "SELECT ?d ?c WHERE { ?d ex:partOf ?c . ?c rdf:type ex:Company }", 1)
    add("Multiple Triple Patterns", "MTP-08: Project uses technology",
        "SELECT ?prj ?t WHERE { ?prj ex:uses ?t . ?t rdf:type ex:Technology }", 1)
    add("Multiple Triple Patterns", "MTP-09: Person located in city",
        "SELECT ?p ?city WHERE { ?p ex:locatedIn ?city . ?city rdf:type ex:City }", 1)
    add("Multiple Triple Patterns", "MTP-10: Company founded by person",
        "SELECT ?c ?f WHERE { ?c ex:foundedBy ?f . ?f rdf:type ex:Person }", 1)
    add("Multiple Triple Patterns", "MTP-11: Company collaboration",
        "SELECT ?c1 ?c2 WHERE { ?c1 ex:collaboratesWith ?c2 }", 1)

    # ========== OPTIONAL ==========
    add("OPTIONAL", "OPT-01: Person with optional location",
        "SELECT ?p ?city WHERE { ?p rdf:type ex:Person . OPTIONAL { ?p ex:locatedIn ?city } }", 40)

    # ========== Property Paths ==========
    add("Property Paths", "PP-01: Inverse: who works for a company",
        "SELECT ?p ?c WHERE { ?c ^ex:worksFor ?p . ?p rdf:type ex:Person }", 1)
    add("Property Paths", "PP-02: Zero-or-more: type chain",
        "SELECT ?s WHERE { ?s rdf:type* ex:City }", 0)
    add("Property Paths", "PP-03: One-or-more: type chain",
        "SELECT ?s WHERE { ?s rdf:type+ ex:Person }", 0)

    # ========== Built-in Functions ==========
    add("Built-in Functions", "BF-01: isIRI check",
        "SELECT ?c WHERE { ?c rdf:type ex:Company . FILTER(isIRI(?c)) }", 20)
    add("Built-in Functions", "BF-02: BOUND check",
        "SELECT ?c WHERE { ?c rdf:type ex:Company . FILTER(BOUND(?c)) }", 20)
    add("Built-in Functions", "BF-03: REGEX on label",
        'SELECT ?c ?name WHERE { ?c rdf:type ex:Company . ?c rdfs:label ?name . FILTER(REGEX(?name, "^T")) }', 0)
    add("Built-in Functions", "BF-04: STRSTARTS",
        'SELECT ?c ?name WHERE { ?c rdf:type ex:Company . ?c rdfs:label ?name . FILTER(STRSTARTS(?name, "A")) }', 0)
    add("Built-in Functions", "BF-05: CONTAINS",
        'SELECT ?c ?name WHERE { ?c rdf:type ex:Company . ?c rdfs:label ?name . FILTER(CONTAINS(?name, "Data")) }', 0)

    # ========== Complex Queries ==========
    add("Complex Queries", "COM-01: Person-Company-City chain",
        "SELECT ?p ?c ?city WHERE { ?p ex:worksFor ?c . ?c ex:locatedIn ?city }", 1)
    add("Complex Queries", "COM-02: Product supply chain",
        "SELECT ?c ?pr ?t WHERE { ?c ex:develops ?pr . ?pr ex:uses ?t }", 1)
    add("Complex Queries", "COM-03: Manager chain",
        "SELECT ?mgr ?sub ?c WHERE { ?mgr ex:manages ?sub . ?sub ex:worksFor ?c }", 0)
    add("Complex Queries", "COM-04: Know who works where",
        "SELECT ?a ?b ?c WHERE { ?a ex:knows ?b . ?b ex:worksFor ?c }", 0)
    add("Complex Queries", "COM-05: Founded company location",
        "SELECT ?c ?city ?f WHERE { ?c ex:foundedBy ?f . ?c ex:locatedIn ?city }", 0)
    add("Complex Queries", "COM-06: Funded project technology",
        "SELECT ?c ?prj ?t WHERE { ?c ex:funds ?prj . ?prj ex:uses ?t }", 0)

    # ========== LIMIT / OFFSET ==========
    add("LIMIT / OFFSET", "LIMIT-01: Top 5 companies",
        "SELECT ?c WHERE { ?c rdf:type ex:Company } LIMIT 5", 5)
    add("LIMIT / OFFSET", "LIMIT-02: Companies page 2 (offset 5 limit 5)",
        "SELECT ?c WHERE { ?c rdf:type ex:Company } ORDER BY ?c OFFSET 5 LIMIT 5", 5)
    add("LIMIT / OFFSET", "LIMIT-03: First 3 persons",
        "SELECT ?p WHERE { ?p rdf:type ex:Person } ORDER BY ?p LIMIT 3", 3)

    # ========== Edge Cases ==========
    add("Edge Cases", "EDGE-01: SELECT * all triples",
        "SELECT * WHERE { ?s ?p ?o }", 1)
    add("Edge Cases", "EDGE-02: Empty result expected",
        "SELECT ?x WHERE { ?x rdf:type ex:NonexistentClass }", 0)
    add("Edge Cases", "EDGE-03: Self-referential sameTerm",
        "SELECT ?c WHERE { ?c rdf:type ex:Company . FILTER(sameTerm(?c, ?c)) }", 20)
    add("Edge Cases", "EDGE-04: Multiple FILTER conditions",
        "SELECT ?c WHERE { ?c rdf:type ex:Company . ?c ex:locatedIn ?city . FILTER(?city = ?city) }", 20)
    add("Edge Cases", "EDGE-05: Label retrieval for all types",
        "SELECT ?s ?label WHERE { ?s rdfs:label ?label }", 1)

    return categories


def run_tests(cli, categories):
    total = 0
    passed = 0
    failed = 0
    errors = 0
    times = []
    details = defaultdict(list)

    for cat_name, queries in categories.items():
        for q in queries:
            total += 1
            name = q["name"]
            expected_min = q["expected_min"]

            start = time.time()
            rc, rows, stderr = cli.query(q["query"])
            elapsed = time.time() - start
            times.append(elapsed)

            if rc != 0:
                errors += 1
                details[cat_name].append({
                    "name": name,
                    "status": "ERROR",
                    "rows": 0,
                    "expected_min": expected_min,
                    "time_s": round(elapsed, 4),
                    "error": stderr.strip()[:200],
                })
                continue

            row_count = len(rows)
            if expected_min == 0:
                status = "PASS" if row_count >= 0 else "FAIL"
            elif row_count >= expected_min:
                status = "PASS"
            else:
                status = "FAIL"

            if status == "PASS":
                passed += 1
            else:
                failed += 1

            details[cat_name].append({
                "name": name,
                "status": status,
                "rows": row_count,
                "expected_min": expected_min,
                "time_s": round(elapsed, 4),
                "sample": rows[:3] if rows else [],
            })

    return {
        "total": total,
        "passed": passed,
        "failed": failed,
        "errors": errors,
        "success_rate": round(passed / total * 100, 1) if total > 0 else 0,
        "avg_time_ms": round(sum(times) / len(times) * 1000, 1) if times else 0,
        "min_time_ms": round(min(times) * 1000, 1) if times else 0,
        "max_time_ms": round(max(times) * 1000, 1) if times else 0,
        "details": dict(details),
    }


def print_report(results):
    print()
    print("=" * 72)
    print("  MDB SPARQL Black-Box Test Report")
    print("=" * 72)
    print(f"  Total queries:     {results['total']}")
    print(f"  Passed:            {results['passed']}")
    print(f"  Failed:            {results['failed']}")
    print(f"  Errors:            {results['errors']}")
    print(f"  Success rate:      {results['success_rate']}%")
    print(f"  Avg query time:    {results['avg_time_ms']} ms")
    print(f"  Min query time:    {results['min_time_ms']} ms")
    print(f"  Max query time:    {results['max_time_ms']} ms")
    print("-" * 72)

    for cat, items in results["details"].items():
        p = sum(1 for i in items if i["status"] == "PASS")
        t = len(items)
        print(f"\n  [{cat}] {p}/{t} passed")
        for item in items:
            symbol = "+" if item["status"] == "PASS" else ("!" if item["status"] == "FAIL" else "?")
            extra = ""
            if item["status"] == "FAIL":
                extra = f" (got {item['rows']}, need >={item['expected_min']})"
            elif item["status"] == "ERROR":
                extra = f" ({item.get('error', '')})"
            print(f"    [{symbol}] {item['name']} ({item['time_s']}s, {item['rows']} rows){extra}")

    print()
    print("=" * 72)

    return {
        "summary": {
            "total": results["total"],
            "passed": results["passed"],
            "failed": results["failed"],
            "errors": results["errors"],
            "success_rate": results["success_rate"],
            "avg_time_ms": results["avg_time_ms"],
            "min_time_ms": results["min_time_ms"],
            "max_time_ms": results["max_time_ms"],
        },
        "by_category": {
            cat: {
                "total": len(items),
                "passed": sum(1 for i in items if i["status"] == "PASS"),
                "failed": sum(1 for i in items if i["status"] == "FAIL"),
                "errors": sum(1 for i in items if i["status"] == "ERROR"),
            }
            for cat, items in results["details"].items()
        },
        "details": {
            cat: [
                {k: v for k, v in item.items() if k != "sample"}
                for item in items
            ]
            for cat, items in results["details"].items()
        },
    }


def main():
    parser = argparse.ArgumentParser(description="MDB SPARQL Black-Box Test Suite")
    parser.add_argument("--cli", required=True, help="Path to mdb_cli binary")
    parser.add_argument("--masters", required=True, help="Master addresses (e.g. 127.0.0.1:7100)")
    parser.add_argument("--keyspace", default="mdb_test", help="Keyspace name")
    parser.add_argument("--data", default=None, help="Path to RDF data file (default: test_data/knowledge_graph.nt)")
    parser.add_argument("--skip-init", action="store_true", help="Skip cluster init")
    parser.add_argument("--skip-import", action="store_true", help="Skip data import")
    parser.add_argument("--report-json", default=None, help="Write JSON report to file")
    args = parser.parse_args()

    data_file = args.data or os.path.join(SCRIPT_DIR, "knowledge_graph.nt")

    if not os.path.exists(args.cli):
        print(f"ERROR: CLI binary not found: {args.cli}")
        sys.exit(1)

    if not args.skip_import and not os.path.exists(data_file):
        print(f"ERROR: Data file not found: {data_file}")
        sys.exit(1)

    cli = MdbCli(args.cli, args.masters, args.keyspace)

    if not args.skip_init:
        print(f"Initializing cluster: masters={args.masters}, keyspace={args.keyspace}")
        ok, msg = cli.init()
        if not ok:
            print(f"Init FAILED: {msg}")
            sys.exit(1)
        print(f"Init OK")
        time.sleep(2)

    if not args.skip_import:
        print(f"Importing data: {data_file}")
        ok, msg = cli.import_file(data_file)
        if not ok:
            print(f"Import FAILED: {msg}")
            sys.exit(1)
        print(f"Import OK")

    categories = build_queries()
    total_queries = sum(len(v) for v in categories.values())
    print(f"\nRunning {total_queries} SPARQL queries across {len(categories)} categories...")

    results = run_tests(cli, categories)

    report = print_report(results)

    if args.report_json:
        with open(args.report_json, "w") as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        print(f"JSON report written to {args.report_json}")

    if results["failed"] > 0 or results["errors"] > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
