# MDB CLI queries

`mdb_cli query '<SPARQL>'` executes one query as before. Statistics remain opt-in
with `--mdb_collect_statistics=true`.

For a read-only workload on an unchanged dataset, put one complete SELECT query
in each UTF-8 file and run:

```sh
mdb_cli --mdb_masters=127.0.0.1:7100 --mdb_keyspace=mdb_test \
  --mdb_collect_statistics=true query-batch first.rq second.rq first.rq
```

A batch connects and loads its dictionary once, collects statistics once when
requested, and executes the files in argument order. Multiline SELECT queries,
comments, PREFIX declarations, and repeated files are allowed. Each query starts
with fresh variables and empty result caches. Client connections and statistics
survive until the process exits; nothing is persisted for another process.

Output uses the ordinary variable/value rows inside numbered frames, including
empty results:

```text
=== query 1 ===
name	value
=== end query 1 ===
=== query 2 ===
=== end query 2 ===
```

All files are checked for readability and SELECT syntax before connecting.
Updates and other query forms are rejected. Semantic or execution failures stop
the batch with a nonzero exit status. Completed earlier frames remain valid;
the failed query has no frame, and later queries do not run.

Do not import or modify the dataset from another process during a batch. Its
dictionary and statistics are not refreshed for external writes, and a batch
does not establish a shared database snapshot. Transaction/snapshot guarantees
are unchanged from the existing single-query path. General SPARQL support has
not expanded; the tested workload uses IRI/plain-literal SELECT BGP queries.

`test_data/validate_batch_bgp.py` compares four-query groups in separate processes
and batches, with statistics off/on, checking every row and duplicate count.
Result caches are cleared between batch queries so this measures connection,
dictionary and statistics reuse without carrying result caches between queries.

## Experimental hash selection

Hash selection is disabled by default. To opt in for a read-only query, use
both `--mdb_enable_hash_join=true` and `--mdb_collect_statistics=true`.
Partition loading must remain enabled.

The candidate supports exactly two unbound patterns of the form
`?s <P> ?m . ?m <Q> ?o`, with three distinct variables. It uses conditional
P/O distinct counts and P/S frequencies to count hits under Q, while global
subject frequencies estimate partition loading before the existing
64-partition fallback. A subject appearing only under another predicate
does not count as a Q hit. Known zero/partial overlap is costed without
short-circuiting execution. Sparse/hot keys, keys absent from all subjects,
missing statistics, external inputs, other join shapes and more than two
patterns retain the existing plan.

Both pair histograms are collected in the same SPO scan. The additional P/S
map holds one entry per distinct predicate/subject pair (at most one per
stored triple); collection remains opt-in. It shares the statistics
invalidation/cache-reset lifecycle and unchanged-dataset assumption. The
selector issues no planning reads. The existing 1.5-times row-work margin
still qualifies candidates. A second rule applies only when both SPO and POS
model query caches are empty: above 64 distinct nonempty subject partitions,
indexed execution needs targeted reads followed by a full-index fallback,
whereas hash reads one fixed predicate partition. In that case lower modeled
hash row work is sufficient. Any SPO/POS cache entry disables this extra rule;
server/OS cache warmth is not inferred. Sparse/hot keys remain excluded.

For this candidate the forward query can route to the fixed P predicate's
POS tablet. The anchor stays restricted to that tablet. Reversed textual
order conservatively keeps existing SPO fragments; no hash choice is made
for those fragments without per-tablet conditional statistics.

The estimated build must fit
`--mdb_hash_join_max_estimated_build_bytes=16777216` and the existing
65,536-row executor cap. The byte flag is a planning threshold, not a hard
allocator limit. Estimates assume unchanged data; stale external statistics
may still lead to an execution error. General HashJoinPlan output/cost
heuristics outside this narrow selector are unchanged.

Each HashJoin additionally enforces `--mdb_hash_join_max_build_bytes=16777216`
at runtime. This independent limit covers requested allocation bytes for row
storage, join keys, hash nodes/buckets, row-index vectors, unbound rows, probe
candidates and temporary keys/rows. Nested containers share one counting
resource; replacement buffers count while the old buffers are still live.
An allocation is rejected before it would exceed the limit. Zero allows only
a build/probe that makes no such allocations; it does not mean unlimited.

This is a per-operator requested-byte budget. It excludes allocator overhead,
fixed plan metadata, isolated input Bindings, child iterators, DocDB/client
caches and final query output; it is not a whole-process RSS limit. The existing
65,536-row cap also applies. The plan captures the byte limit at construction
and preserves it through clone. A smaller runtime budget can reject a plan
accepted by the separate planning estimate.

Build or probe allocation rejection returns a non-OK iterator status, releases
the counted buffers and remains an error on reset. A successful reset releases
the preceding build before rebuilding against new outer input. Ordinary CLI
queries return nonzero without publishing query rows; query-batch stops before
framing the failed query and does not run later queries. Earlier successful
batch frames remain valid. There is no spill or automatic retry.

## Predicate-conditioned estimates and diagnostics

With statistics enabled, a fixed subject and predicate with a variable object
uses the stored P/S count, including zero for an absent pair. Input-bound object
variables still use an average selectivity; their actual values are unavailable
while planning. Fully bound triples retain the existing P/O estimate and
independent subject factor. No planning reads or additional statistics scan is
introduced. Clone and cached estimates follow the statistics version.

For a small controlled join comparison, run each of the four individual
`HashSelectionCostsUnrelatedPredicateSubjects`,
`HashSelectionUsesPartialPredicateSubjectOverlap`,
`HashSelectionKeepsFanoutWithPredicateOverlap` and
`HashSelectionKeepsHotPredicateKeysIndexed` integration tests through
`yb_build.sh` with `MDB_MEASURE_PREDICATE_OVERLAP=1`. The optional measurement
executes both join methods on the same persisted POS anchor for four alternating
trials, clearing model query caches before each run. Logs report statistics
collection, QL requests, execution time and HashJoin's counted peak bytes.
Every result bag is checked; there is no speed assertion. Timings exclude
statistics collection, ownership discovery, memory diagnostics and oracle sort,
but include iterator construction, reads and result production. Server/OS caches
are not flushed, and the counted hash bytes are not process RSS.

## Known input keys for index joins

With statistics enabled, an IndexNestedLoopPlan joining two complete
fixed-predicate scans can use the preceding key distribution. For a chain
`?s <P> ?m . ?m <Q> ?o` it sums P/O frequencies times Q/S frequencies;
the reverse order uses the same intersection. This preserves duplicate
products and includes zero or partial overlap, without assuming every input
key belongs to Q. RHS scan/emit work uses these hits; loading still includes
all subject rows, empty-partition rereads and the 64-key full-index fallback.
The prefix load estimate uses a mean because order/cache reuse is unknown.

This requires three distinct variables, no external input on the preceding
scan, no RHS anchor, and no preceding anchor other than its fixed POS
predicate. Unknown/composite input distributions, SPO/RHS fragments and other
shapes retain the previous estimator. Standalone scans with unknown bindings
also retain their old average. No new statistics, reads, flags, join executor
or routing change is introduced. Join plans capture their estimates at
construction; rebuild after refreshing statistics rather than cloning a stale
join plan and expecting it to refresh.

For a small diagnostic run each of the four overlap integration tests above
with `MDB_MEASURE_BOUND_INPUT=1` through yb_build.sh. It compares a forced
forward IndexNestedLoop order with the optimizer in four alternating trials,
clearing model caches and checking all result bags. Hash selection is disabled
for those measurements. Timing includes planning/reads/output, not statistics
or oracle sorting. The baseline is an explicitly forced forward plan,
not an old executable or a complete before/after version comparison. No timing assertion is used.

The ordinary CLI's SPO-fragment path keeps its conservative estimates. The
file regression also exercises the unfragmented local path using a high LIMIT
that exceeds the full result count, with statistics off/on and both pattern
orders. This does not establish general LIMIT/OFFSET distributed support.

## Hash selection scale diagnostics

Generate the deterministic 25,000-triple fixture using
`python3 src/yb/mdb/test_data/validate_hash_join_scale.py --generate <dataset.nt>`.
It contains 5,000 distinct-key probes, 5,000 probes over eight hot keys, a
5,000-row RHS and 10,000 noise triples.

Run the existing `MdbFileE2ETest.CliDs10000NonTransactional` case through
`yb_build.sh`, setting `MDB_SCALE_PYTHON` to an absolute Python executable,
`MDB_SCALE_DRIVER` to the absolute `validate_hash_join_scale.py` path,
`MDB_SCALE_DATASET` to the generated file and `MDB_SCALE_OUTPUT_DIR` to a
new directory. The driver runs two reversed-order repetitions of statistics
off, statistics on and hash opt-in, each as four fresh CLI processes and a
four-query batch. It checks complete result multisets, routing, hash selection
and one statistics collection per enabled process. GNU time must be available.

The JSON and raw outputs include process wall time, CPU, CLI peak RSS and
sampled MiniCluster process RSS. CLI time includes initialization, dictionary,
statistics, execution and output. Its hash comparison includes POS routing
changes. These measurements do not isolate hash allocation or cold storage.

`MdbDistributedIntegrationTest.HashJoinSamePosScaleComparison` separately
compares explicit HashJoin and IndexNestedLoop on the same physical POS anchor,
with empty model caches before every run. It checks all rows across four
alternating trials, validates the selector's distinct/hot decisions, and logs
execution time, QL read requests and the existing build-memory lower bound.
Neither diagnostic has a speed threshold or changes optimizer defaults.

### Catalog counts and unknown statistics

The eleven RdfCatalog count methods now read the existing RdfYBModel statistics.
They accept storage IDs and return optional counts: no collected or invalidated
statistics means nullopt; a present zero means a known empty relation or missing
term. These accessors do not issue reads or retain their own snapshot. Counts
inherit the existing external-write freshness and concurrency limitations.

Without collected statistics, TriplePlan uses the existing 1000-row heuristic
and binding selectivity factors instead of treating the old catalog stubs as
known zeros. This is a planning estimate, not a stored count or an empty-result
shortcut. Statistics collection and hash selection remain opt-in. Legacy planner
sources are adapted to optional counts but are not part of the active MDB build.

### Cached Leapfrog traversal before planner integration

The DocDB cache adapter now enumerates every suffix row under the current fixed
and intersection prefix, preserving bag multiplicity. Seeking is monotone at one
trie depth; normal exhaustion can be restarted by reopening or backtracking and
descending again. Enumeration reset uses a separate cursor. Caches must remain
unchanged during traversal. Initial ranges must be fixed point prefixes, with at
most three columns and distinct variable roles. Cached tuples and initial terms
use storage IDs; dictionary translation and repeated-variable equality must be
handled before a future planner constructs these iterators.

LeapfrogIter exposes a sticky status, including cancellation. The adapter accepts
an optional initial load status, which a future DocDB loader must pass explicitly;
the adapter itself does not perform RPCs. LeapfrogJoin checks child errors at
open, traversal and enumeration, including errors behind an earlier empty child.
Invalid layouts fail before traversal. Normal EOF remains successful; errors
survive begin/reset. No new concurrency or cache snapshot guarantee is provided.

Triangle tests compare independent natural-join bags across all six variable
orders, multiple densities, zero and UINT64_MAX, and repeated execution. These
are cache-backed tests. The restricted DocDB integration below is an explicit
opt-in; the general Leapfrog optimizer and cardinality estimator are not enabled.
try_estimate now appends exact cached tuple counts at a positioned intersection:
the total under its parent prefix and the suffix at/after its current key,
including the current key and duplicate rows. It searches the immutable sorted
cache without loading data or changing traversal/enumeration cursors. Counts are
not distinct-key counts, a join-output estimate, or a remote freshness guarantee.
Invalid/unopened/non-intersection/exhausted/error/cancelled states, changed cache
content/ownership, mismatched or aliased output vectors, and counts beyond exact
double integer representation return false without appending. Reopen to estimate
a new cache generation or input prefix. Legacy cursor movement does not invalidate
counts. The generic estimator now samples only through open_for_estimation:
unloaded DocDB scans return NotSupported without loading, while already loaded
scans reopen their existing cache. Unknown inputs, execution errors/cancellation,
normal EOF and malformed progress counts remain distinguishable through
EstimateVarCost. Sampling needs an explicit binding size and constant initial
ranges, and restarts its seek budget for each call. It owns and repositions its
iterators; callers must not share active traversal. Its legacy negative-cost
wrapper still permits planner fallback. Completed sampling retains zero heuristic
cost, including nonempty completed intersections; this is not cardinality. General
planning and automatic algorithm/variable-order selection remain disabled.


### Opt-in DocDB Leapfrog triangles

Use `--mdb_enable_leapfrog_join=true` with mdb_cli to select Leapfrog for a basic
graph pattern of exactly three fixed-predicate triples over three distinct free
subject/object variables. Each variable must occur in two different pairs, forming
a triangle. The default is false. Outer bindings, repeated variables in one triple,
variable predicates, fixed subject/object terms, other graph shapes and active
write transactions retain the existing planner. Unsupported shapes fall back;
execution errors remain errors.

The first relation reads only its fragment's physical tablet. Other relations
may read across tablets. The anchor uses explicit TableRange pagination with a
server-side fixed-P filter on the original physical index column. Other relations
use explicit paged reads of a fixed predicate hash key. Both use
--mdb_leapfrog_read_page_rows (1..65536), default 1024. The flag now also controls
the anchor; previous versions kept it at 128. Set the flag to 128 for smaller
pages on both paths. Restoring the former 128-anchor/1024-other mix requires
reverting this change; there is no separate anchor setting.
An invalid size fails before reading rather than returning an empty result or
selecting another algorithm. Larger pages can increase transient RPC/arena
memory, which tuple-capacity counters do not measure. No wire or storage format
changes are involved; Leapfrog remains disabled by default.
Complete relations are
loaded and reordered as PSO or POS to match one
deterministic global variable order. Trie keys remain storage IDs; a separate
binding translates final rows to packed ObjectIds, preserving dictionary mappings
and projected duplicates. Normal reset reuses the loaded relation; create a new
query for fresh reads. Loading errors and cancellation remain sticky.

This is a functional integration, not cost-based Leapfrog selection. The anchor
keeps its physical tablet boundary and receives only matching predicate rows; the
server may still inspect unrelated rows. Other relations materialize complete
predicate partitions. Reuse has a retention budget described
below, but no query-wide memory limit, spill, common read snapshot or remote
parallel execution. General Leapfrog cost
estimation, default selection and performance claims require separate validation.

A selected query logs `MDB Leapfrog triangle: relations=3`. Unsupported queries
keep the existing plan even when the flag is set.


### Query-scoped predicate reuse

Each sequential ScatterExecutor::Execute owns a fresh reuse scope. Successful
complete non-anchor relations are keyed by table ID, target permutation and
storage predicate ID, then shared as immutable caches with independent iterator
cursors. The anchor's physical tablet is never admitted. Failed or partial
loads remain errors and are never published. Scope exit, including an error,
releases retained entries; iterators that already borrowed them retain ownership.
Nested scopes restore the outer cache. This does not make the existing model or
query context safe for concurrent queries.

--mdb_leapfrog_reuse_bytes defaults to 8388608 (8 MiB); zero disables admission.
At most 64 relations are retained, including empty ones. A relation exceeding the
remaining tuple-vector capacity allowance stays private and execution continues.
Admission moves the completed vector without copying rows. This bounds retained
tuple storage only: loading may allocate beyond the allowance before admission,
and map/allocator/RPC/arena/result memory is excluded. It is not a query memory
budget, spill mechanism or RSS limit.

A new Execute reads afresh, including after writes with the same parsed plan.
Within one execution, later fragments reuse the first successful read; no
cross-query cache or common DocDB read snapshot is provided. Active write
transactions remain outside the supported Leapfrog path.

### Controlled triangle comparisons

mdb_leapfrog_docdb_test includes four debug-only GreedyLeapfrog*Comparison cases:
sparse one-to-one, dense complete, skewed shared keys and an empty closing
intersection. Each case compares four rotating rounds of the current Greedy
path and opt-in Leapfrog with reuse disabled/enabled at the same 1024-row
page size over the same four SPO fragments (48 full bags across the
four cases). Statistics and Hash selection are disabled. Every timed result is
checked against an independent complete bag; there are no timing pass/fail
thresholds.

MDB_TRIANGLE_PERF records routing, plan construction, reads, gathering and row
extraction time plus QL requests. Import, oracle construction and sorting are
outside that interval. Query caches are cleared between runs; server/OS caches
and retained vector capacities are not flushed. This measures instrumented debug
execution with small synthetic data, not an old executable or fresh-process CLI.

The comparisons enable --mdb_profile_leapfrog (default false) and collect the
MDBLeapfrogTriangle::Phases debug callback. Loading time includes RPC/session,
decoding and loading management; per-row conversion, reordering and insertion
time is reported separately as build time and is subtracted from load_other.
Join includes trie setup and enumeration; output is final storage-to-packed ID
mapping. Routing, gathering and other residual work remain outside those phase
counters but inside the total. Each scan measures only actual loading; reset
reuses its cache. Per-row clock overhead affects these instrumented measurements.
No phase is reported as pure disk I/O or as an allocator/RSS measurement.

AllocatedRowBytes and QueryCacheMemory count reserved tuple-vector storage only,
excluding map nodes, RPC/arena buffers, result bindings and process overhead.
The Leapfrog materialization callback reports live private tuples and all
query-retained shared tuples separately. Each sample sums those once and tracks
their simultaneous maximum across sequential fragments. The query-scope callback
reports hits, misses, admissions, rejections and retained rows/bytes. Reuse hits
are excluded from actual relation-load and newly loaded-row counts.
The model cache total includes retained empty buffers; logs also record the
starting capacity and incremental growth. These counters are not RSS or memory
limits and should not be used alone to choose a production budget.

### Explicit 25K retention and process-memory experiments

Set MDB_LEAPFROG_SCALE_RUN=1 to enable the debug-only Leapfrog25k*RetentionComparison
cases. Sparse and skewed triangles use 8192 left nodes, with unrelated triples
padding each graph to exactly 25000 inputs; full bags contain 8192 and 16384 rows.
Four rotating rounds compare private loading, 8 MiB retention and a 192 KiB
allowance admitting only one non-anchor relation. All use 1024-row
pages and four SPO fragments. Counts, phases and reserved tuple capacity are
recorded; no timing thresholds or automatic plan-selection rules are used.

The CliLeapfrog25k*MemoryComparison cases run validate_leapfrog_scale.py with
MDB_SCALE_PYTHON, MDB_LEAPFROG_SCALE_DRIVER and MDB_LEAPFROG_SCALE_OUTPUT configured.
They precreate four tablets per RDF index and import separate deterministic IRI
graphs with matching relation cardinalities, verify all six physical index row
counts and compare each fresh CLI's
full output bag with the independent reference in validate_scale_bgp.py.
GNU time records wall, CPU and peak RSS for the CLI process only. Import is
reported separately; parent servers and the Python reference are outside that
RSS measurement. Startup, dictionary restoration and output are inside CLI wall
time. Profiling clocks are off in the CLI experiment but on in the separate
numeric-ID counter experiment. Their samples must not be pooled. Server/OS
caches are not flushed, and retained tuple bytes are not an RSS limit.

### Incremental cache construction

DocDBRangeCache keeps unsigned (c0, c1, c2) order after every InsertRow and
preserves duplicate rows. Nondecreasing input appends directly; an out-of-order
tuple uses a binary lower bound before vector insertion. This avoids scanning
the accumulated prefix on ordered predicate pages. Arbitrary input may still
shift a linear number of rows per insertion; this is not a general bulk-sort
algorithm or a new memory budget. Existing scan cursors assume the cache remains
unchanged during traversal, and Clear/Reset retain their previous contracts.
