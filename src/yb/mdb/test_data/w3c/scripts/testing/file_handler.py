import json
import re
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
from xml.etree import ElementTree

import rdflib
import rdflib.collection
import rdflib.term

from . import options
from .logging import Level, log
from .options import CWD, INTERNAL_TESTS, ROOT_MANIFESTS, TEST_SUITES
from .types import BadTest, Binding, Bindings, QueryTest, Result, Test, TestSuite, Triple, Value

MF = rdflib.URIRef("http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#")
QT = rdflib.URIRef("http://www.w3.org/2001/sw/DataAccess/tests/test-query#")
DAWGT = rdflib.URIRef("http://www.w3.org/2001/sw/DataAccess/tests/test-dawg#")
UT = rdflib.URIRef("http://www.w3.org/2009/sparql/tests/test-update#")
RDF_TYPE = rdflib.URIRef("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")


def node_to_path(node: rdflib.term.Node, manifest_dir: Path) -> Path:
    if isinstance(node, rdflib.URIRef):
        uri_str = str(node)
        if "://" in uri_str:
            _, path_str = uri_str.split("://", maxsplit=1)
            path = Path(path_str)
            if path.is_file():
                return path
            resolved = manifest_dir / path.name
            if resolved.is_file():
                return resolved
            return path
        else:
            return Path(manifest_dir / uri_str)
    return Path(str(node))


def get_manifests_from_root_manifest(root_path: Path) -> List[Path]:
    graph = rdflib.Graph()
    graph.parse(str(root_path), format="turtle")

    manifest_dir = root_path.parent

    main_nodes = list(graph.subjects(predicate=RDF_TYPE, object=MF + "Manifest"))
    if not main_nodes:
        return []

    main_node = main_nodes[0]

    includes = list(graph.objects(subject=main_node, predicate=MF + "include"))
    if not includes:
        return [root_path]

    paths_bnode = includes[0]
    path_nodes: List[rdflib.term.Node] = list(rdflib.collection.Collection(graph, paths_bnode))

    path_list: List[Path] = []
    for path_node in path_nodes:
        p = node_to_path(path_node, manifest_dir)
        if p.exists():
            path_list.append(p)
        else:
            log(Level.WARNING, f"Manifest not found: {p}")

    return path_list


def get_tests_from_manifest(manifest_path: Path) -> List[Test]:
    graph = rdflib.Graph()
    graph.parse(str(manifest_path), format="turtle")

    manifest_dir = manifest_path.parent
    tests: List[Test] = []

    eval_test_type = MF + "QueryEvaluationTest"
    pos_syntax_type = MF + "PositiveSyntaxTest11"
    neg_syntax_type = MF + "NegativeSyntaxTest11"
    dawg_pos_syntax = DAWGT + "PositiveSyntaxTest"
    dawg_neg_syntax = DAWGT + "NegativeSyntaxTest"

    for test_node in graph.subjects(predicate=RDF_TYPE, object=eval_test_type):
        action_node = next(graph.objects(subject=test_node, predicate=MF + "action"), None)
        if action_node is None:
            continue

        query_node = next(graph.objects(subject=action_node, predicate=QT + "query"), None)
        if query_node is None:
            q_uri = action_node
        else:
            q_uri = query_node

        query = node_to_path(q_uri, manifest_dir)

        data_node = next(graph.objects(subject=action_node, predicate=QT + "data"), None)
        data = node_to_path(data_node, manifest_dir) if data_node is not None else None

        result_node = next(graph.objects(subject=test_node, predicate=MF + "result"), None)
        if result_node is None:
            continue

        expected = node_to_path(result_node, manifest_dir)
        if not expected.exists():
            log(Level.WARNING, f"Expected file not found: {expected}")
            continue

        graph_output = False
        query_text = query.read_text() if query.exists() else ""
        if any(kw in query_text.lower() for kw in ["describe", "construct"]):
            graph_output = True

        tests.append(QueryTest(
            query=query,
            expected=expected,
            data=data,
            graph_output=graph_output,
        ))

    for bad_type in [neg_syntax_type, dawg_neg_syntax]:
        for test_node in graph.subjects(predicate=RDF_TYPE, object=bad_type):
            action_node = next(graph.objects(subject=test_node, predicate=MF + "action"), None)
            if action_node is None:
                continue
            query = node_to_path(action_node, manifest_dir)
            if query.exists():
                tests.append(BadTest(query=query))

    for ok_type in [pos_syntax_type, dawg_pos_syntax]:
        for test_node in graph.subjects(predicate=RDF_TYPE, object=ok_type):
            action_node = next(graph.objects(subject=test_node, predicate=MF + "action"), None)
            if action_node is None:
                continue
            query = node_to_path(action_node, manifest_dir)
            if query.exists():
                tests.append(QueryTest(query=query, expected=Path("/dev/null")))

    return tests


def get_tests_from_root_manifest(root_manifest: Path) -> TestSuite:
    log(Level.BEGIN, f"ROOT MANIFEST: {root_manifest.relative_to(CWD)}")

    test_list: List[Test] = []
    manifests = get_manifests_from_root_manifest(root_manifest)

    for manifest in manifests:
        log(Level.BEGIN, f"MANIFEST: {manifest.relative_to(CWD)}")
        for test in get_tests_from_manifest(manifest):
            log(Level.DEBUG, str(test))
            test_list.append(test)
        log(Level.END, f"MANIFEST: {manifest.relative_to(CWD)} ({len([t for t in test_list if str(manifest) in str(getattr(t, 'query', ''))])} tests)")

    log(Level.END, f"ROOT MANIFEST: {root_manifest.relative_to(CWD)}")
    name = str(root_manifest.relative_to(TEST_SUITES))
    return TestSuite(name, test_list)


def query_has_keywords(query_path: Path, keywords: List[str]) -> bool:
    try:
        text = query_path.read_text().lower()
    except Exception:
        return False
    return any(kw.lower() in text for kw in keywords)


def check_supported(test: Test) -> Tuple[bool, Optional[str]]:
    if isinstance(test, BadTest):
        return True, None
    assert isinstance(test, QueryTest)
    query_text = test.query.read_text()
    for keyword in options.NOT_SUPPORTED_KEYWORDS:
        if keyword.upper() in query_text.upper():
            return False, f"not supported keyword: {keyword}"
    return True, None


def get_select_result_from_file(expected_file: Path) -> Tuple[Set[str], List[Dict[str, Value]]]:
    suffix = expected_file.suffix
    if suffix == ".srx":
        return get_select_result_from_srx(expected_file)
    elif suffix in (".ttl", ".rdf"):
        return get_select_result_from_rdf(expected_file)
    elif suffix == ".csv":
        return get_select_result_from_csv(expected_file)
    elif suffix == ".tsv":
        return get_select_result_from_tsv(expected_file)
    elif suffix == ".srj":
        return get_select_result_from_srj(expected_file)
    else:
        raise ValueError(f"Unknown expected file format: {suffix}")


def get_select_result_from_srx(expected_file: Path) -> Tuple[Set[str], List[Dict[str, Value]]]:
    tree = ElementTree.parse(str(expected_file))
    root = tree.getroot()
    ns = "http://www.w3.org/2005/sparql-results#"

    variables: Set[str] = set()
    for el in root.iter(f"{{{ns}}}variable"):
        name = el.get("name")
        if name:
            variables.add(name)

    bindings: List[Dict[str, Value]] = []
    for result_el in root.iter(f"{{{ns}}}result"):
        binding: Dict[str, Value] = {}
        for child in result_el:
            tag = child.tag.split("}")[-1] if "}" in child.tag else child.tag
            var_name = child.get("name", "")
            val_text = child.text or ""

            if tag == "uri":
                binding[var_name] = Value(value=val_text, type_=Value.Type.IRI, datatype="", language="")
            elif tag == "literal":
                datatype = child.get("datatype", "")
                lang = child.get(f"{{{ns}}}lang", "")
                binding[var_name] = Value(value=val_text, type_=Value.Type.LITERAL, datatype=datatype, language=lang)
            elif tag == "bnode":
                binding[var_name] = Value(value=val_text, type_=Value.Type.BNODE, datatype="", language="")
            else:
                binding[var_name] = Value(value=val_text, type_=Value.Type.UNKNOWN, datatype="", language="")
        if binding:
            bindings.append(binding)
    return variables, bindings


def get_select_result_from_rdf(expected_file: Path) -> Tuple[Set[str], List[Dict[str, Value]]]:
    g = rdflib.Graph()
    g.parse(str(expected_file), format="turtle")
    RS = rdflib.Namespace("http://www.w3.org/2005/sparql-results#")

    variables: Set[str] = set()
    for _, _, var in g.triples((None, RS["variable"], None)):
        variables.add(str(var))

    solution_nodes = list(g.subjects(RDF_TYPE, RS["solution"]))
    bindings: List[Dict[str, Value]] = []
    for node in solution_nodes:
        binding: Dict[str, Value] = {}
        for _, _, binding_node in g.triples((node, RS["binding"], None)):
            var_name = str(g.value(binding_node, RS["variable"]))
            value_node = g.value(binding_node, RS["value"])
            if value_node is not None:
                binding[var_name] = _rdf_term_to_value(value_node)
        if binding:
            bindings.append(binding)
    return variables, bindings


def _rdf_term_to_value(term) -> Value:
    if isinstance(term, rdflib.URIRef):
        return Value(value=str(term), type_=Value.Type.IRI, datatype="", language="")
    elif isinstance(term, rdflib.Literal):
        lang = term.language or ""
        datatype = str(term.datatype) if term.datatype else ""
        return Value(value=str(term), type_=Value.Type.LITERAL, datatype=datatype, language=lang)
    elif isinstance(term, rdflib.BNode):
        return Value(value=str(term), type_=Value.Type.BNODE, datatype="", language="")
    else:
        return Value(value=str(term), type_=Value.Type.UNKNOWN, datatype="", language="")


def get_select_result_from_csv(expected_file: Path) -> Tuple[Set[str], List[Dict[str, Value]]]:
    import csv
    content = expected_file.read_text()
    reader = csv.DictReader(content.splitlines())
    variables = set(reader.fieldnames or [])
    bindings: List[Dict[str, Value]] = []
    for row in reader:
        binding: Dict[str, Value] = {}
        for var, val in row.items():
            binding[var] = _parse_text_value(val)
        bindings.append(binding)
    return variables, bindings


def get_select_result_from_tsv(expected_file: Path) -> Tuple[Set[str], List[Dict[str, Value]]]:
    lines = expected_file.read_text().strip().split("\n")
    if not lines:
        return set(), []
    headers = lines[0].split("\t")
    variables = set(headers)
    bindings: List[Dict[str, Value]] = []
    for line in lines[1:]:
        vals = line.split("\t")
        binding: Dict[str, Value] = {}
        for i, var in enumerate(headers):
            if i < len(vals):
                binding[var] = _parse_text_value(vals[i])
        if binding:
            bindings.append(binding)
    return variables, bindings


def get_select_result_from_srj(expected_file: Path) -> Tuple[Set[str], List[Dict[str, Value]]]:
    data = json.loads(expected_file.read_text())
    variables = set(data.get("head", {}).get("vars", []))
    bindings: List[Dict[str, Value]] = []
    for b in data.get("results", {}).get("bindings", []):
        binding: Dict[str, Value] = {}
        for var, vd in b.items():
            binding[var] = _parse_srj_value(vd)
        if binding:
            bindings.append(binding)
    return variables, bindings


def _parse_srj_value(vd: dict) -> Value:
    typ = vd.get("type", "literal")
    val = vd.get("value", "")
    if typ == "uri":
        return Value(value=val, type_=Value.Type.IRI, datatype="", language="")
    elif typ == "literal":
        dt = vd.get("datatype", "")
        lang = vd.get("xml:lang", "")
        return Value(value=val, type_=Value.Type.LITERAL, datatype=dt, language=lang)
    elif typ == "bnode":
        return Value(value=val, type_=Value.Type.BNODE, datatype="", language="")
    else:
        return Value(value=str(val), type_=Value.Type.UNKNOWN, datatype="", language="")


def _parse_text_value(val: str) -> Value:
    val = val.strip()
    if val.startswith("<") and val.endswith(">"):
        return Value(value=val[1:-1], type_=Value.Type.IRI, datatype="", language="")
    elif val.startswith("_:"):
        return Value(value=val, type_=Value.Type.BNODE, datatype="", language="")
    elif val.startswith('"'):
        content = val[1:]
        datatype = ""
        language = ""
        ci = content.rfind("^^<")
        if ci != -1:
            datatype = content[ci + 3:].rstrip(">")
            content = content[:ci]
            qe = content.rfind('"')
            if qe != -1:
                content = content[:qe]
        else:
            ai = content.rfind('"@')
            if ai != -1:
                language = content[ai + 2:]
                content = content[:ai]
            else:
                qe = content.rfind('"')
                if qe != -1:
                    content = content[:qe]
        return Value(value=content, type_=Value.Type.LITERAL, datatype=datatype, language=language)
    else:
        return Value(value=val, type_=Value.Type.UNKNOWN, datatype="", language="")


def get_ask_result_from_file(expected_file: Path) -> bool:
    suffix = expected_file.suffix
    if suffix == ".srx":
        return get_ask_result_from_srx(expected_file)
    elif suffix in (".ttl", ".rdf"):
        return get_ask_result_from_ttl(expected_file)
    elif suffix == ".srj":
        return get_ask_result_from_srj(expected_file)
    else:
        return get_ask_result_from_srx(expected_file)


def get_ask_result_from_srx(expected_file: Path) -> bool:
    return "<boolean>true</boolean>" in expected_file.read_text()


def get_ask_result_from_srj(expected_file: Path) -> bool:
    data = json.loads(expected_file.read_text())
    return data.get("boolean", False)


def get_ask_result_from_ttl(expected_file: Path) -> bool:
    g = rdflib.Graph()
    g.parse(str(expected_file), format="turtle")
    RS = rdflib.Namespace("http://www.w3.org/2005/sparql-results#")
    for _, _, o in g.triples((None, RS["boolean"], None)):
        return str(o).lower() == "true"
    return False


def get_graph_result_from_ttl(expected_file: Path) -> List[Triple]:
    g = rdflib.Graph()
    g.parse(str(expected_file), format="turtle")
    triples: List[Triple] = []
    for s, p, o in g:
        triples.append((_rdf_term_to_value(s), _rdf_term_to_value(p), _rdf_term_to_value(o)))
    return triples


def canonize_triples(triples: List[Triple]) -> Set[Tuple[str, str, str]]:
    return {(s.value, p.value, o.value) for s, p, o in triples}


def get_official_tests() -> List[TestSuite]:
    test_suites: List[TestSuite] = []
    for manifest_path in ROOT_MANIFESTS:
        if manifest_path.exists():
            suite = get_tests_from_root_manifest(manifest_path)
            test_suites.append(suite)
    return test_suites


def get_internal_tests() -> List[TestSuite]:
    suites: List[TestSuite] = []
    if not INTERNAL_TESTS.exists():
        return suites
    for test_dir in sorted(INTERNAL_TESTS.iterdir()):
        if not test_dir.is_dir():
            continue
        good_dir = test_dir / "good_queries"
        bad_dir = test_dir / "bad_queries"
        tests: List[Test] = []
        if good_dir.exists():
            for rq_file in sorted(good_dir.glob("*.rq")):
                tests.append(QueryTest(query=rq_file, expected=rq_file.with_suffix(".expected")))
        if bad_dir.exists():
            for rq_file in sorted(bad_dir.glob("*.rq")):
                tests.append(BadTest(query=rq_file))
        if tests:
            suites.append(TestSuite(name=f"internal/{test_dir.name}", tests=tests))
    return suites
