#!/usr/bin/env python3
"""Generate ds10000 dataset + 22 grammar-covering queries, one per syntax type."""

import random
import json
import os
import re

random.seed(42)

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

PREFIX = "http://example.org/"
RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
RDFS_LABEL = "http://www.w3.org/2000/01/rdf-schema#label"

ENTITY_TYPES = [
    "Person", "Company", "Product", "City", "Department",
    "Project", "Technology", "Event", "Organization", "Service",
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
    "schedules", "dispatches",
]
