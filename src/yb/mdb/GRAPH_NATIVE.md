# Bounded graph fragment execution

This experimental path adds GraphExpand operators above the existing RDF tables
and DocDB transactions. The default remains the existing optimized executor.
It is a one/two-hop execution path, not a complete graph-native database or a
replacement for SPARQL property-path semantics.

## Supported scope

| Area | Scope |
|---|---|
| Seed | One bounded VALUES relation or an anchor triple scan |
| Expansion | Fixed predicate, bound endpoint, outgoing SPO or incoming OPS |
| Composition | One/two expansions, binding compatibility, duplicates and self-loops |
| Results | RDF term identity, projection order, existing outer filter/DISTINCT/order/slice |
| Distribution | Partition-owned seed tasks, client-routed adjacency reads, shared child snapshot |
| Failure | Explicit errors, bounded buffered state, no partial public success |

VALUES accepts at most 256 rows. Programs allow 124 real variable slots plus four
constant scratch slots; graph pages allow at most 256 rows. The pipeline bounds
intermediate seeds and shares exact `(start, bound neighbor)` reads within a
batch. It restores every source row and witness; it does not deduplicate inputs.

Variable predicates, unknown/UNDEF VALUES, multiple VALUES inputs, unsupported
expressions or nested filters, property paths and explicit exchange/sampling
retain the existing capability fallback before worker execution. This is not a
claim that the old engine lacks these language features.

## Selection and diagnostics

- `--mdb_graph_mode=old`: existing optimized worker/coordinator path.
- `--mdb_graph_mode=new`: graph execution for accepted shapes; unsupported shapes
  retain pre-execution capability fallback. Worker targets are required.
- `--mdb_graph_mode=auto`: experimental bounded reuse heuristic. Exact VALUES or
  one predicate-hash POS page supplies evidence; insufficient evidence keeps old.
- Omit the new flag to preserve `--mdb_graph_native` compatibility. Its default is
  false. CLI rejects a true legacy boolean combined with explicit old/auto.

Auto examines at most 128 exact VALUES rows or one explicit data read returning
at most 16 unfiltered POS rows. Scan evidence is grouped by the actual SPO task
owner, not shared across worker fragments. Scan probing retains old when there
are more than four tasks or a bound anchor endpoint. Historical statistics do
not provide a freshness promise and do not drive this decision. Probe and
execution share the same query read view. Probe errors terminate the query.

`graph_execution_decision` describes the chosen candidate and its reason/cost;
actual execution also requires `graph_native_workers`, fallback counters and a
successful query status. `graph_expand_rows` counts fragment output bindings,
not internal hops. Read-operation and protobuf payload counters are not physical
RPC/TCP byte counters. SSH loopback addresses do not prove data locality.

## Upgrade/Rollback safety

Graph OPEN requires envelope v2, graph capability v1 and an explicit graph ACK.
An old worker rejects it. Ordinary old-path traffic remains v1, accepted by the
new worker. Unknown optional fields alone never select the graph executor.
Stage all public graph results until workers exit and child results are applied;
late errors publish no public rows/success end. Do not replay old execution after
remote work or a probe error.

Deploy a complete matching runtime, including the generated proto DSO and
metadata. Wire compatibility does not imply generated C++ ABI compatibility.
Rebuild programs using WorkerOptions after its mode field changes. To roll back,
disable graph mode and use a complete previous versioned runtime; remove the new
flag when invoking an old CLI. No persistent RDF/dictionary format changes are
introduced by this round. Keep the previous runtime and commit history.

The public Gather default budget remains 64 MiB; worker buffered state defaults
to 8 MiB. Large intermediates can fail even with a small final LIMIT. These are
buffer accounting budgets, not total RSS limits. Default-off behavior is retained.

## Validation and reproduction

Build with the repository entry point and the existing compiler/build root:

```bash
./yb_build.sh release --clang21 --ninja -j2 --sj --skip-pg-parquet \
  --no-odyssey --no-ybc --target mdb_fragment_integration_test
```

`test_data/graph_native_acceptance_cases.txt` lists the 21 new targeted GTests.
Run one listed case per invocation, for example:

```bash
./yb_build.sh release --clang21 --ninja --skip-cxx-build \
  --cxx-test mdb_fragment_integration_test \
  --gtest_filter MdbFragmentIntegrationTest.GraphNativePublicFrozenTypedBagAndModifiers
```

The mixed-version test needs `MDB_GRAPH_LEGACY_WORKER` pointing to a complete
verified pre-graph worker launcher; without it the test skips, which is not a
compatibility pass. Direct tests exercise batch/page edges, both directions,
typed identity, duplicate witnesses, snapshot/control errors and recovery.
Public/CLI tests exercise selection, global modifiers, capability rejection and
failure output. Recorded phases used different source snapshots; they are not
one simultaneous final-version full-suite run. The final delivery build also
passed all 21 listed local GTests, including the supplied legacy-worker case;
this does not rerun earlier cross-host performance or fault experiments.

Portable performance input and full-result validation:

```bash
python3 src/yb/mdb/test_data/graph_native_benchmark.py generate --output /tmp/graph-bench
python3 src/yb/mdb/test_data/graph_native_benchmark.py validate \
  --manifest /tmp/graph-bench/dataset-manifest.json --case twohop512 \
  --result /tmp/twohop-result.json
```

Configure masters, keyspace, worker targets/binary and SSH config for the test
environment. Use identical data, topology, RF, page 16, worker batch 128, maximum
two workers, memory budgets and full JSON output across old/new/auto. Warm each
case/mode once, then rotate case and mode order for at least five formal samples.
Validate every complete typed bag; retain all parameters, raw results, profiles,
per-process RSS and cleanup evidence. Resource collection and oracle validation
belong outside the timed query boundary. Do not overwrite prior runs.

## Recorded bounded results and remaining coverage

The measured source snapshot was `b25a6b9866e6` (content fingerprint, not commit).
It used one master, three RF3 tservers on two hosts (2+1), SSH RPC forwarding,
and one query at a time. Both hosts executed real graph workers. Concurrent
commit, channel loss, worker/Gather budgets, active cross-host leader transfer
and a 4-to-5 tablet split were verified on this bounded path in earlier phases.
Final delivery formatting and two equivalent non-runtime flag declarations
postdate the performance snapshot; use recorded runtime hashes to reproduce it.

The performance fixture has 3,720 unique triples. Formal medians in seconds:

| Shape / full rows | Old wall | New wall | Auto wall |
|---|---:|---:|---:|
| Shared1024 / 8192 | 5.383 | 4.782 | 4.982 |
| Unique1024 / 1024 | 5.788 | 5.836 | 7.341 |
| Twohop512 / 2048 | 12.356 | 5.133 | 4.990 |

Two-hop new execution achieved 2.41x wall and 4.26x adapter-execution improvement;
observed data reads fell from 2564 to 433. Shared ranges overlap, unique new/old
are similar, and auto unique regresses 26.8%. Universal/automatic performance
success was not achieved. Keep old as default and auto experimental.

The early contract mentioned 4096-origin unique/shared/fanout and incoming
performance shapes. The timed R2-09 contract instead froze the smaller three
shapes above before measurement. The original 4096/fanout matrix and incoming
performance were not run and remain open coverage; incoming correctness has
separate tests. Do not claim the early larger matrix passed. Larger intermediates
also need explicit budget admission. This is a bounded experimental delivery.

QLever v0.6.0 with the same input returned full HTTP results at median
54.568/10.630/18.178 ms, respectively. It was a persistent single-node service;
MDB used RF3, two hosts and fresh per-query workers. This reference does not
establish equal-guarantee distributed rankings or a current SOTA gap.

Arbitrary-length paths, graph storage redesign, generic shuffle, persistent
workers, master HA, whole-host failures, scale/concurrency/cold-cache studies and
general SPARQL certification remain outside this delivery. Evidence is archived
under the separate round2-r201 through round2-r210 run directories; raw artifacts
contain environment-specific paths and are not part of the source tree.
