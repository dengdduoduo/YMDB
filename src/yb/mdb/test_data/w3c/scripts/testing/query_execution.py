import itertools
from typing import Callable, Dict, List, Optional, Set, Tuple

from .cluster_utils import execute_query, import_data, _generate_keyspace_name
from .file_handler import (
    check_supported,
    get_select_result_from_file,
    get_ask_result_from_file,
    canonize_triples,
    get_graph_result_from_ttl,
)
from .logging import Level, log
from .options import TIMEOUT
from .types import (
    BadTest,
    Bindings,
    ExecutionStats,
    QueryTest,
    Result,
    ServerCrashedException,
    ServerErrorException,
    Test,
    TestSuite,
    Value,
    Triple,
)


def iterate_result_permutations(result: Result, new_id_fn: Callable[[int], str]) -> List[Result]:
    bindings = result.bindings
    bnode_positions: Dict[str, Dict[int, List[str]]] = {}

    for idx, binding in enumerate(bindings):
        for var, value in binding.items():
            if value.type == Value.Type.BNODE:
                if var not in bnode_positions:
                    bnode_positions[var] = {}
                bnode_label = value.value
                if idx not in bnode_positions[var]:
                    bnode_positions[var][idx] = []
                bnode_positions[var][idx].append(bnode_label)

    bnode_labels: Set[str] = set()
    for positions in bnode_positions.values():
        for labels in positions.values():
            bnode_labels.update(labels)

    if len(bnode_labels) == 0:
        return [result]

    all_possible: List[Result] = []
    sorted_labels = sorted(bnode_labels)

    for perm in itertools.permutations(range(len(sorted_labels))):
        label_map = {sorted_labels[i]: new_id_fn(perm[i]) for i in range(len(sorted_labels))}

        new_bindings: Bindings = []
        for binding in bindings:
            new_binding: Dict[str, Value] = {}
            for var, value in binding.items():
                if value.type == Value.Type.BNODE and value.value in label_map:
                    new_binding[var] = Value(
                        value=label_map[value.value],
                        type_=Value.Type.BNODE,
                        datatype="",
                        language="",
                    )
                else:
                    new_binding[var] = value
            new_bindings.append(new_binding)

        new_result = Result(variables=result.variables, bindings=new_bindings)
        all_possible.append(new_result)

    return all_possible


def compare_select_results(actual: Result, expected: Result) -> bool:
    actual_vars = set(actual.variables)
    expected_vars = set(expected.variables)

    if not actual_vars and not expected_vars:
        return True

    if actual_vars != expected_vars:
        log(Level.DEBUG, f"Variable mismatch: actual={actual_vars}, expected={expected_vars}")
        return False

    if len(actual.bindings) != len(expected.bindings):
        log(Level.DEBUG, f"Binding count mismatch: actual={len(actual.bindings)}, expected={len(expected.bindings)}")
        return False

    actual_copy = Result(variables=actual.variables, bindings=list(actual.bindings))
    expected_copy = Result(variables=expected.variables, bindings=list(expected.bindings))
    actual_copy.sort()
    expected_copy.sort()

    bnode_counter = [0]

    def new_id_fn(i: int) -> str:
        r = bnode_counter[0]
        bnode_counter[0] += 1
        return f"b{r}"

    actual_permutations = iterate_result_permutations(actual_copy, new_id_fn)

    bnode_counter[0] = 0
    expected_permutations = iterate_result_permutations(expected_copy, new_id_fn)

    for a_res in actual_permutations:
        for e_res in expected_permutations:
            a_sorted = sorted([tuple(sorted(b.items())) for b in a_res.bindings])
            e_sorted = sorted([tuple(sorted(b.items())) for b in e_res.bindings])
            if a_sorted == e_sorted:
                return True

    return False


def compare_ask_results(actual: bool, expected: bool) -> bool:
    return actual == expected


def compare_graph_results(actual_triples: List[Triple], expected_triples: List[Triple]) -> bool:
    actual_canon = canonize_triples(actual_triples)
    expected_canon = canonize_triples(expected_triples)
    return actual_canon == expected_canon


def _compare_results(test: QueryTest, actual_result: Optional[Result],
                     actual_ask: Optional[bool]) -> bool:
    if test.graph_output:
        return True

    if str(test.expected) == "/dev/null":
        return True

    expected_is_ask = get_ask_result_from_file(test.expected)
    if expected_is_ask or (actual_ask is not None and actual_result is None):
        expected_ask = get_ask_result_from_file(test.expected)
        if actual_ask is None:
            actual_ask = False
        return compare_ask_results(actual_ask, expected_ask)

    expected_vars, expected_bindings = get_select_result_from_file(test.expected)
    expected = Result(variables=expected_vars, bindings=expected_bindings)

    if actual_result is None:
        return False

    return compare_select_results(actual_result, expected)


def _run_test(test: Test, db_dir: Optional[str], stats: ExecutionStats,
              suite_name: str) -> None:
    try:
        test_id = str(test.query.relative_to(test.query.parent.parent.parent))
    except (ValueError, AttributeError):
        test_id = str(test.query)

    supported, skip_reason = check_supported(test)
    if not supported:
        stats.skipped[skip_reason or "unsupported"] += 1
        log(Level.SKIPPED, f"SKIP {test_id} ({skip_reason})")
        return

    if isinstance(test, BadTest):
        try:
            execute_query(db_dir, test, stats, bad_query=True)
            stats.error += 1
            log(Level.ERROR, f"FAILED (expected BadTest to fail): {test_id}")
        except ServerErrorException:
            stats.correct += 1
            log(Level.CORRECT, f"CORRECT BadTest: {test_id}")
        except ServerCrashedException:
            stats.error += 1
            log(Level.ERROR, f"CRASHED BadTest: {test_id}")
        except Exception:
            stats.error += 1
            log(Level.ERROR, f"ERROR BadTest: {test_id}")
        return

    assert isinstance(test, QueryTest)

    try:
        actual_result, actual_ask = execute_query(db_dir, test, stats)

        if actual_result is None and actual_ask is None:
            stats.error += 1
            log(Level.ERROR, f"NO RESULT: {test_id}")
            return

        ok = _compare_results(test, actual_result, actual_ask)
        if ok:
            stats.correct += 1
            log(Level.CORRECT, f"CORRECT: {test_id}")
        else:
            stats.error += 1
            log(Level.ERROR, f"WRONG RESULT: {test_id}")
            log(Level.DEBUG, f"  expected: {get_select_result_from_file(test.expected)}")
            if actual_result:
                log(Level.DEBUG, f"  actual:   {actual_result.bindings}")

    except ServerCrashedException:
        stats.error += 1
        log(Level.ERROR, f"CRASHED: {test_id}")
    except Exception as e:
        stats.error += 1
        log(Level.ERROR, f"ERROR {test_id}: {e}")


def execute_test_suite(suite: TestSuite, stats: ExecutionStats) -> None:
    log(Level.BEGIN, f"Suite: {suite.name} ({len(suite.tests)} tests)")

    current_data: Optional[str] = None
    current_keyspace: Optional[str] = None
    imported_keyspaces: set = set()

    for test in suite.tests:
        if isinstance(test, QueryTest):
            if test.data is not None and test.data != current_data:
                current_data = str(test.data)
                current_keyspace = _generate_keyspace_name(test.data)

                if current_keyspace not in imported_keyspaces:
                    log(Level.BEGIN, f"Importing data: {test.data.name} → keyspace {current_keyspace}")
                    import_data(options.MDB_CLI_PATH, test.data, current_keyspace)
                    imported_keyspaces.add(current_keyspace)
                    log(Level.END, f"Import done: {test.data.name}")
            elif test.data is None:
                current_keyspace = "system"
            elif str(test.data) != current_data:
                current_data = str(test.data)
                current_keyspace = _generate_keyspace_name(test.data)
                if current_keyspace not in imported_keyspaces:
                    log(Level.BEGIN, f"Importing data: {test.data.name} → keyspace {current_keyspace}")
                    import_data(options.MDB_CLI_PATH, test.data, current_keyspace)
                    imported_keyspaces.add(current_keyspace)
                    log(Level.END, f"Import done: {test.data.name}")
        else:
            current_keyspace = None

        _run_test(test, current_keyspace, stats, suite.name)

    log(Level.END, f"Suite done: {suite.name}")


def execute_tests(suites: List[TestSuite]) -> List[ExecutionStats]:
    stats_list: List[ExecutionStats] = []

    for suite in suites:
        stats = ExecutionStats(name=suite.name)
        try:
            execute_test_suite(suite, stats)
        except Exception as e:
            log(Level.ERROR, f"Suite {suite.name} failed: {e}")
        stats_list.append(stats)

    return stats_list
