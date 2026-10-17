#!/usr/bin/env python3
"""Generate ds100000: 100K nodes, 50K edges, 100 relations → 250K triples."""
import random, os

random.seed(42)
PREFIX = "http://example.org/"
RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
RDFS_LABEL = "http://www.w3.org/2000/01/rdf-schema#label"

TYPES = ["Person","Company","Product","City","Department",
         "Project","Technology","Event","Organization","Service"]

PREDS = ["worksFor","locatedIn","manages","knows","developedBy",
    "contains","reportsTo","memberOf","fundedBy","collaboratesWith",
    "produces","sells","buys","owns","partnersWith",
    "reports","supervises","advises","mentors","sponsors",
    "competesWith","dependsOn","implements","designs","tests",
    "deploys","maintains","operates","supports","integratesWith",
    "regulates","audits","certifies","trains","evaluates",
    "plans","executes","monitors","reviews","approves",
    "submits","validates","documents","publishes","distributes",
    "requires","provides","consumes","generates","processes",
    "transforms","stores","retrieves","archives","secures",
    "authenticates","authorizes","encrypts","decrypts","hashes",
    "encodes","decodes","compresses","decompresses","serializes",
    "deserializes","formats","parses","renders","visualizes",
    "compares","aligns","merges","splits","filters",
    "aggregates","sorts","indexes","ranks","scores",
    "predicts","classifies","clusters","recommends","optimizes",
    "schedules","dispatches","notifies","escalates","resolves",
    "measures","analyzes","reportsOn","governs","oversees",
    "investsIn","acquires","divests","licenses","outsources"]

N = 100000
E = 50000
RP = PREDS[:100]
out = os.path.join(os.path.dirname(__file__), "dataset_ds100000.nt")
with open(out, "w") as f:
    for i in range(1, N+1):
        t = TYPES[i % 10]
        uri = f"{PREFIX}{t}{i}"
        f.write(f"<{uri}> <{RDF_TYPE}> <{PREFIX}{t}> .\n")
        label = f"{t} #{i}"
        f.write(f"<{uri}> <{RDFS_LABEL}> \"{label}\" .\n")

    uris = [f"{PREFIX}{TYPES[i%10]}{i+1}" for i in range(N)]
    for _ in range(E):
        s = uris[random.randint(0,N-1)]
        o = uris[random.randint(0,N-1)]
        p = RP[random.randint(0,99)]
        f.write(f"<{s}> <{PREFIX}{p}> <{o}> .\n")
    f.write("\n")

size_mb = os.path.getsize(out) / 1e6
print(f"ds100000: {N} nodes, {E} edges → {N*2+E:,} triples, {size_mb:.1f}MB → {out}")
