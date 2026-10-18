#!/usr/bin/env python3
"""
SPARQL Query Generator for benchmarking.
Generates random valid SPARQL queries covering all supported syntax:
  - BGP (1, 2, 3 triple patterns)
  - FILTER: isIRI, BOUND, REGEX, STRSTARTS, CONTAINS, sameTerm, =
  - OPTIONAL
  - UNION
  - Property Paths: ^, *, +
  - ORDER BY (ASC/DESC)
  - DISTINCT
  - LIMIT, OFFSET
"""

import random
import json
import os
import sys

random.seed(42)


def iri(s):
    return f"<http://example.org/{s}>"


QUERY_TEMPLATES = {
    "BGP_1": {
        "category": "Basic Graph Pattern",
        "desc": "Single triple pattern",
        "weight": 10,
    },
    "BGP_2": {
        "category": "Basic Graph Pattern",
        "desc": "Two triple patterns (join)",
        "weight": 10,
    },
    "BGP_3": {
        "category": "Basic Graph Pattern",
        "desc": "Three triple patterns (complex join)",
        "weight": 5,
    },
    "FILTER_isIRI": {
        "category": "FILTER",
        "desc": "FILTER with isIRI",
        "weight": 5,
    },
    "FILTER_BOUND": {
        "category": "FILTER",
        "desc": "FILTER with BOUND",
        "weight": 5,
    },
    "FILTER_REGEX": {
        "category": "FILTER",
        "desc": "FILTER with REGEX on label",
        "weight": 4,
    },
    "FILTER_eq": {
        "category": "FILTER",
        "desc": "FILTER with variable equality",
        "weight": 4,
    },
    "FILTER_neq": {
        "category": "FILTER",
        "desc": "FILTER with variable inequality",
        "weight": 4,
    },
    "FILTER_STRSTARTS": {
        "category": "FILTER",
        "desc": "FILTER with STRSTARTS",
        "weight": 3,
    },
    "FILTER_CONTAINS": {
        "category": "FILTER",
        "desc": "FILTER with CONTAINS",
        "weight": 3,
    },
    "FILTER_sameTerm": {
        "category": "FILTER",
        "desc": "FILTER with sameTerm",
        "weight": 3,
    },
    "OPTIONAL": {
        "category": "OPTIONAL",
        "desc": "OPTIONAL pattern",
        "weight": 5,
    },
    "UNION": {
        "category": "UNION",
        "desc": "UNION of two patterns",
        "weight": 5,
    },
    "PATH_inverse": {
        "category": "Property Path",
        "desc": "Inverse property path (^)",
        "weight": 3,
    },
    "PATH_star": {
        "category": "Property Path",
        "desc": "Zero-or-more path (*)",
        "weight": 2,
    },
    "PATH_plus": {
        "category": "Property Path",
        "desc": "One-or-more path (+)",
        "weight": 2,
    },
    "ORDER_ASC": {
        "category": "ORDER BY",
        "desc": "ORDER BY ASC",
        "weight": 4,
    },
    "ORDER_DESC": {
        "category": "ORDER BY",
        "desc": "ORDER BY DESC",
        "weight": 4,
    },
    "DISTINCT": {
        "category": "DISTINCT",
        "desc": "SELECT DISTINCT",
        "weight": 4,
    },
    "LIMIT": {
        "category": "LIMIT/OFFSET",
        "desc": "LIMIT clause",
        "weight": 4,
    },
    "LIMIT_OFFSET": {
        "category": "LIMIT/OFFSET",
        "desc": "LIMIT with OFFSET",
        "weight": 2,
    },
    "COMPLEX_mixed": {
        "category": "Complex",
        "desc": "FILTER + OPTIONAL + ORDER BY",
        "weight": 2,
    },
    "COMPLEX_union_filter": {
        "category": "Complex",
        "desc": "UNION with FILTER",
        "weight": 2,
    },
    "COMPLEX_path_filter": {
        "category": "Complex",
        "desc": "Property path with FILTER",
        "weight": 2,
    },
    "COMPLEX_opt_distinct": {
        "category": "Complex",
        "desc": "OPTIONAL + DISTINCT + ORDER BY",
        "weight": 2,
    },
}

ENTITY_TYPES = [
    "Person", "Company", "Product", "City", "Department",
    "Project", "Technology", "Event", "Organization", "Service",
]


class SparqlGenerator:
    def __init__(self, scale_info, seed=42):
        self.nodes = scale_info["nodes"]
        self.edges = scale_info["edges"]
        self.relations = scale_info["relations"]
        self.pred_names = scale_info["pred_names"]
        self.entity_types = scale_info["entity_types"]
        random.seed(seed)

    def var(self, v):
        return f"?{v}"

    def rdf_type(self):
        return f"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"

    def rdfs_label(self):
        return f"<http://www.w3.org/2000/01/rdf-schema#label>"

    def rand_iri(self):
        etype = random.choice(self.entity_types)
        idx = random.randint(1, self.nodes)
        return iri(f"{etype}{idx}")

    def rand_pred(self):
        return iri(random.choice(self.pred_names))

    def gen_select(self, vars_list, distinct=False):
        sel = "SELECT DISTINCT " if distinct else "SELECT "
        return sel + " ".join(self.var(v) for v in vars_list)

    def gen_where_open(self):
        return "WHERE {"

    def gen_where_close(self):
        return "}"

    def gen_order(self, var, asc=True):
        d = "ASC" if asc else "DESC"
        return f"ORDER BY {d}({self.var(var)})"

    def gen_limit(self, n):
        return f"LIMIT {n}"

    def gen_offset(self, n):
        return f"OFFSET {n}"

    def bgp_1(self):
        s = self.var("x")
        p = self.rand_pred()
        o = self.var("y")
        return self.gen_select(["x", "y"]), [f"{s} {p} {o} ."]

    def bgp_2(self):
        s = self.var("x")
        p1 = self.rand_pred()
        o = self.var("y")
        p2 = self.rand_pred()
        z = self.var("z")
        triples = [
            f"{s} {p1} {o} .",
            f"{o} {p2} {z} .",
        ]
        return self.gen_select(["x", "y", "z"]), triples

    def bgp_3(self):
        x = self.var("x")
        p1 = self.rand_pred()
        y = self.var("y")
        p2 = self.rand_pred()
        z = self.var("z")
        p3 = self.rand_pred()
        w = self.var("w")
        triples = [
            f"{x} {p1} {y} .",
            f"{y} {p2} {z} .",
            f"{z} {p3} {w} .",
        ]
        return self.gen_select(["x", "y", "z", "w"]), triples

    def filter_isIRI(self):
        s = self.var("x")
        p = self.rand_pred()
        o = self.var("y")
        triples = [f"{s} {p} {o} ."]
        filt = f"FILTER(isIRI({s}))"
        return self.gen_select(["x", "y"]), triples, [filt]

    def filter_BOUND(self):
        s = self.var("x")
        p = self.rand_pred()
        o = self.var("y")
        triples = [f"{s} {p} {o} ."]
        filt = f"FILTER(BOUND({o}))"
        return self.gen_select(["x", "y"]), triples, [filt]

    def filter_REGEX(self):
        s = self.var("x")
        lab = self.rdfs_label()
        label = self.var("l")
        triples = [f"{s} {lab} {label} ."]
        filt = f'FILTER(REGEX({label}, "Person|Company"))'
        return self.gen_select(["x", "l"]), triples, [filt]

    def filter_eq(self):
        s = self.var("x")
        p1 = self.rand_pred()
        y = self.var("y")
        p2 = self.rand_pred()
        z = self.var("z")
        triples = [
            f"{s} {p1} {y} .",
            f"{s} {p2} {z} .",
        ]
        filt = f"FILTER({y} = {z})"
        return self.gen_select(["x", "y"]), triples, [filt]

    def filter_neq(self):
        s = self.var("x")
        p1 = self.rand_pred()
        y = self.var("y")
        p2 = self.rand_pred()
        z = self.var("z")
        triples = [
            f"{s} {p1} {y} .",
            f"{s} {p2} {z} .",
        ]
        filt = f"FILTER({y} != {z})"
        return self.gen_select(["x", "y", "z"]), triples, [filt]

    def filter_STRSTARTS(self):
        s = self.var("x")
        lab = self.rdfs_label()
        label = self.var("l")
        triples = [f"{s} {lab} {label} ."]
        filt = f'FILTER(STRSTARTS({label}, "Person"))'
        return self.gen_select(["x", "l"]), triples, [filt]

    def filter_CONTAINS(self):
        s = self.var("x")
        lab = self.rdfs_label()
        label = self.var("l")
        triples = [f"{s} {lab} {label} ."]
        filt = f'FILTER(CONTAINS({label}, "#"))'
        return self.gen_select(["x", "l"]), triples, [filt]

    def filter_sameTerm(self):
        s = self.var("x")
        p1 = self.rand_pred()
        y = self.var("y")
        p2 = self.rand_pred()
        z = self.var("z")
        triples = [
            f"{s} {p1} {y} .",
            f"{s} {p2} {z} .",
        ]
        filt = f"FILTER(sameTerm({y}, {z}))"
        return self.gen_select(["x", "y"]), triples, [filt]

    def optional(self):
        x = self.var("x")
        p1 = self.rand_pred()
        y = self.var("y")
        p2 = self.rand_pred()
        label = self.var("l")
        lab = self.rdfs_label()
        triples = [f"{x} {p1} {y} ."]
        opt = [f"{y} {lab} {label} ."]
        parts = ["\n".join(f"  {t}" for t in triples)]
        parts.append("OPTIONAL {")
        parts.extend(f"    {t}" for t in opt)
        parts.append("}")
        return self.gen_select(["x", "y", "l"]), ["\n".join(parts)]

    def union(self):
        x = self.var("x")
        y = self.var("y")
        tp = self.rdf_type()
        etype1 = random.choice(self.entity_types)
        etype2 = random.choice([e for e in self.entity_types if e != etype1])
        parts = ["{"]
        parts.append(f"  {x} {tp} {iri(etype1)} .")
        parts.append("} UNION {")
        parts.append(f"  {y} {tp} {iri(etype2)} .")
        parts.append("}")
        return self.gen_select(["x", "y"]), ["\n".join(parts)]

    def path_inverse(self):
        x = self.var("x")
        y = self.var("y")
        pred = self.rand_pred()
        triples = [f"{x} ^{pred} {y} ."]
        return self.gen_select(["x", "y"]), triples

    def path_star(self):
        x = self.var("x")
        y = self.var("y")
        pred = self.rand_pred()
        triples = [f"{x} {pred}* {y} ."]
        return self.gen_select(["x", "y"]), triples

    def path_plus(self):
        x = self.var("x")
        y = self.var("y")
        pred = self.rand_pred()
        triples = [f"{x} {pred}+ {y} ."]
        return self.gen_select(["x", "y"]), triples

    def order_asc(self):
        x = self.var("x")
        p = self.rand_pred()
        y = self.var("y")
        triples = [f"{x} {p} {y} ."]
        order = self.gen_order("x", True)
        limit = self.gen_limit(20)
        return self.gen_select(["x", "y"]), triples, [], f"{order}\n{limit}"

    def order_desc(self):
        x = self.var("x")
        p = self.rand_pred()
        y = self.var("y")
        triples = [f"{x} {p} {y} ."]
        order = self.gen_order("x", False)
        limit = self.gen_limit(20)
        return self.gen_select(["x", "y"]), triples, [], f"{order}\n{limit}"

    def distinct(self):
        x = self.var("x")
        p = self.rand_pred()
        triples = [f"{x} {p} {iri('Person' + str(random.randint(1, self.nodes)))} ."]
        return self.gen_select(["x"], distinct=True), triples

    def limit_only(self):
        x = self.var("x")
        p = self.rand_pred()
        y = self.var("y")
        triples = [f"{x} {p} {y} ."]
        limit = self.gen_limit(10)
        return self.gen_select(["x", "y"]), triples, [], limit

    def limit_offset(self):
        x = self.var("x")
        p = self.rand_pred()
        y = self.var("y")
        triples = [f"{x} {p} {y} ."]
        mods = f"{self.gen_limit(10)}\n{self.gen_offset(5)}"
        return self.gen_select(["x", "y"]), triples, [], mods

    def complex_mixed(self):
        x = self.var("x")
        p1 = self.rand_pred()
        y = self.var("y")
        lab = self.rdfs_label()
        label = self.var("l")
        p2 = self.rand_pred()
        z = self.var("z")
        triples = [f"{x} {p1} {y} ."]
        opt = [f"{y} {lab} {label} ."]
        filt = f'FILTER(BOUND({label}) && REGEX({label}, "Person"))'
        parts = ["\n".join(f"  {t}" for t in triples)]
        parts.append("OPTIONAL {")
        parts.extend(f"    {t}" for t in opt)
        parts.append("}")
        filt_lines = [filt]
        order = self.gen_order("x", True)
        limit = self.gen_limit(15)
        return self.gen_select(["x", "y", "l"]), ["\n".join(parts)], filt_lines, f"{order}\n{limit}"

    def complex_union_filter(self):
        x = self.var("x")
        y = self.var("y")
        tp = self.rdf_type()
        etype1 = "Person"
        etype2 = "Company"
        parts = ["{"]
        parts.append(f"  {x} {tp} {iri(etype1)} .")
        parts.append(f"  FILTER(isIRI({x}))")
        parts.append("} UNION {")
        parts.append(f"  {y} {tp} {iri(etype2)} .")
        parts.append(f"  FILTER(isIRI({y}))")
        parts.append("}")
        return self.gen_select(["x", "y"]), ["\n".join(parts)]

    def complex_path_filter(self):
        x = self.var("x")
        y = self.var("y")
        pred = self.rand_pred()
        triples = [f"{x} {pred}* {y} ."]
        filt = f"FILTER(isIRI({x}) && isIRI({y}))"
        limit = self.gen_limit(20)
        return self.gen_select(["x", "y"]), triples, [filt], limit

    def complex_opt_distinct(self):
        x = self.var("x")
        p = self.rand_pred()
        y = self.var("y")
        lab = self.rdfs_label()
        label = self.var("l")
        triples = [f"{x} {p} {y} ."]
        opt = [f"{x} {lab} {label} ."]
        parts = ["\n".join(f"  {t}" for t in triples)]
        parts.append("OPTIONAL {")
        parts.extend(f"    {t}" for t in opt)
        parts.append("}")
        order = self.gen_order("x", True)
        limit = self.gen_limit(20)
        return self.gen_select(["x"], distinct=True), ["\n".join(parts)], [], f"{order}\n{limit}"

    _TEMPLATE_METHODS = {
        "BGP_1": "bgp_1",
        "BGP_2": "bgp_2",
        "BGP_3": "bgp_3",
        "FILTER_isIRI": "filter_isIRI",
        "FILTER_BOUND": "filter_BOUND",
        "FILTER_REGEX": "filter_REGEX",
        "FILTER_eq": "filter_eq",
        "FILTER_neq": "filter_neq",
        "FILTER_STRSTARTS": "filter_STRSTARTS",
        "FILTER_CONTAINS": "filter_CONTAINS",
        "FILTER_sameTerm": "filter_sameTerm",
        "OPTIONAL": "optional",
        "UNION": "union",
        "PATH_inverse": "path_inverse",
        "PATH_star": "path_star",
        "PATH_plus": "path_plus",
        "ORDER_ASC": "order_asc",
        "ORDER_DESC": "order_desc",
        "DISTINCT": "distinct",
        "LIMIT": "limit_only",
        "LIMIT_OFFSET": "limit_offset",
        "COMPLEX_mixed": "complex_mixed",
        "COMPLEX_union_filter": "complex_union_filter",
        "COMPLEX_path_filter": "complex_path_filter",
        "COMPLEX_opt_distinct": "complex_opt_distinct",
    }

    def generate_query(self, template_name):
        method_name = self._TEMPLATE_METHODS.get(template_name)
        if method_name is None:
            return None
        method = getattr(self, method_name, None)
        if method is None:
            return None

        try:
            result = method()
            if len(result) == 4:
                select_clause, triples, filters, modifiers = result
            elif len(result) == 3:
                select_clause, triples, filters = result
                modifiers = ""
            else:
                select_clause, triples = result
                filters = []
                modifiers = ""

            lines = [select_clause]
            lines.append(self.gen_where_open())

            for t in triples:
                if "\n" in t:
                    lines.append(t)
                else:
                    lines.append(f"  {t}")

            for f in filters:
                lines.append(f"  {f}")

            lines.append(self.gen_where_close())
            if modifiers:
                lines.append(modifiers)

            return "\n".join(lines)
        except Exception as e:
            return None


def generate_comprehensive_queries(gen, templates, num_extra=7):
    """Generate queries ensuring every template is represented at least once."""
    queries = []
    seen_templates = set()

    # Phase 1: include every template once
    for template in templates:
        for attempt in range(5):
            q = gen.generate_query(template)
            if q and q not in {x["query"] for x in queries}:
                queries.append({
                    "id": len(queries) + 1,
                    "template": template,
                    "category": QUERY_TEMPLATES[template]["category"],
                    "description": QUERY_TEMPLATES[template]["desc"],
                    "query": q,
                })
                seen_templates.add(template)
                break

    # Phase 2: fill extras with weighted random
    weights = [QUERY_TEMPLATES[t]["weight"] for t in templates]
    for i in range(num_extra):
        for attempt in range(20):
            template = random.choices(templates, weights=weights, k=1)[0]
            q = gen.generate_query(template)
            if q and q not in {x["query"] for x in queries}:
                queries.append({
                    "id": len(queries) + 1,
                    "template": template,
                    "category": QUERY_TEMPLATES[template]["category"],
                    "description": QUERY_TEMPLATES[template]["desc"],
                    "query": q,
                })
                break

    return queries


def main():
    out_dir = os.path.join(os.path.dirname(__file__), "queries")
    os.makedirs(out_dir, exist_ok=True)

    scales = [
        {"name": "ds100", "nodes": 100, "edges": 50, "relations": 5},
        {"name": "ds200", "nodes": 200, "edges": 100, "relations": 10},
        {"name": "ds500", "nodes": 500, "edges": 250, "relations": 25},
        {"name": "ds1000", "nodes": 1000, "edges": 500, "relations": 50},
        {"name": "ds2000", "nodes": 2000, "edges": 1000, "relations": 100},
    ]

    PREDICATE_NAMES = [
        "worksFor", "locatedIn", "manages", "knows", "developedBy",
        "contains", "reportsTo", "memberOf", "fundedBy", "collaboratesWith",
        "produces", "sells", "buys", "owns", "partnersWith",
        "reports", "supervises", "advises", "mentors", "sponsors",
        "competesWith", "dependsOn", "implements", "designs", "tests",
        "deploys", "maintains", "operates", "supports", "integratesWith",
        "regulates", "audits", "certifies", "trains", "evaluates",
        "plans", "executes", "monitors", "reviews", "approves",
        "submits", "validates", "documents", "publishes", "distributes",
        "requires", "provides", "consumes", "generates", "processes",
        "transforms", "stores", "retrieves", "archives", "secures",
        "authenticates", "authorizes", "encrypts", "decrypts", "hashes",
        "encodes", "decodes", "compresses", "decompresses", "serializes",
        "deserializes", "formats", "parses", "renders", "visualizes",
        "compares", "aligns", "merges", "splits", "filters",
        "aggregates", "sorts", "indexes", "ranks", "scores",
        "predicts", "classifies", "clusters", "recommends", "optimizes",
        "schedules", "dispatches", "notifies", "escalates", "resolves",
        "measures", "analyzes", "reportsOn", "governs", "oversees",
        "investsIn", "acquires", "divests", "licenses", "outsources",
    ]
    ENTITY_TYPES = [
        "Person", "Company", "Product", "City", "Department",
        "Project", "Technology", "Event", "Organization", "Service",
    ]

    templates = list(QUERY_TEMPLATES.keys())

    for scale in scales:
        scale_info = {
            "nodes": scale["nodes"],
            "edges": scale["edges"],
            "relations": scale["relations"],
            "pred_names": PREDICATE_NAMES[:scale["relations"]],
            "entity_types": ENTITY_TYPES,
        }
        gen = SparqlGenerator(scale_info, seed=42)

        queries = generate_comprehensive_queries(gen, templates, num_extra=7)

        out_path = os.path.join(out_dir, f"queries_{scale['name']}.json")
        with open(out_path, "w") as f:
            json.dump(queries, f, indent=2)

        cats = {}
        for q in queries:
            cats[q["category"]] = cats.get(q["category"], 0) + 1
        print(f"  {scale['name']}: {len(queries)} queries")
        for cat, count in sorted(cats.items()):
            print(f"    {cat}: {count}")
        print(f"    → {out_path}")


if __name__ == "__main__":
    main()
