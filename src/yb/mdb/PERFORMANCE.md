# Query measurement

`mdb_cli --mdb_query_profile=true ... query '<SPARQL>'` writes JSON records
prefixed `MDB_QUERY_PROFILE` and `MDB_CLI_TIMING` to stderr. Result stdout is
unchanged. `YBCMdbGetLastQueryProfile()` exposes the query record to API callers.
Fields are additive; a zero counter can mean that its path was not used.

All `*_us` fields use a monotonic wall clock, not CPU time. The adapter reports
parsing/rewriting, read-view setup, dictionary restoration, planning, execution,
and total time. Execution includes result callbacks and, where applicable,
fragment gathering, exchange, and global finalization. Total excludes outer
adapter locking and final read-context destruction.

`storage_flush_us` and `storage_decode_us` describe coordinator data pages;
they do not include dictionary restoration. Remote counters describe spawning,
waiting for OPEN, waiting for pull responses, consuming rows, and cleanup.
These fields are nested inside execution, not additional total time. Workers
may run concurrently. OPEN waiting includes worker client/table/transaction
setup; pull waiting includes worker reads, joins, and transport. Neither is a
measurement of pure worker CPU time.

`remote_read_operations` sums differences of each fragment's cumulative read
counter. Local and remote read-operation counts are not wire RPC packet counts
and do not include every metadata operation during initialization.

The CLI separately reports initialization, optional statistics collection,
execution, publication, total RunQuery time, and status. External process timing
also includes loader/flag processing and shutdown. Do not mix fresh CLI timings
with an already initialized API endpoint. Report the complete result boundary,
build flags, resources, cache conditions, and initialization policy.

The `MdbStage9BaselineTest` cases are opt-in diagnostic fixtures. Set
`MDB_STAGE9_DATASETS` to the generated six-file dataset directory; without it
the cases are skipped, not validated. `MDB_STAGE9_REPEATS` controls repetitions
(default five), separate from the first observation. Run each case individually
through `yb_build.sh`, using the matching build root. Correctness assertions
check the complete result multiset and schema before a sample is accepted.

Generate the default six datasets with:

```sh
python3 src/yb/mdb/test_data/gen_stage9_baseline.py --output /path/to/baseline-data
```

The generator records hashes and refuses to replace differing files. Pass that
directory through MDB_STAGE9_DATASETS when invoking one baseline case with
yb_build.sh. A matching hash identifies input bytes; it is not a correctness or
performance result by itself.

## Shutdown and rejected samples

The adapter owns its cluster client. Call YBCMdbShutdown before process teardown
to close the model and client while thread-local infrastructure is alive. The
CLI does this on every exit path. Calls are serialized with queries; callbacks
must not re-enter shutdown. Repeated successful shutdown calls are safe.

MDB transaction handles retain their manager until their last strong reference
is destroyed, including references held by asynchronous read callbacks after
a synchronous flush returns. Shutdown first stops manager work; the manager is
not freed while those transactions still need its RPC registry. Worker shutdown
also drains its owned client before manager/client member destruction.

Reject samples with a nonzero process exit even if output was already produced.
Keep pre-fix data separate when a lifecycle fix changes measured binaries.

Reinitializing the public adapter releases its old model before replacing the
owned client, including when changing namespaces. It preserves configured query
options on success. An initialization error discards partially opened tables,
dictionary, model, and client; retry does not reuse a dangling client pointer.
These changes do not optimize dictionary scans or conditional writes.


## Dictionary initialization

Legacy migration validates the complete legacy mapping against a strong scan
of authoritative term claims before publishing missing entries. Already known
claims need no per-term conditional write or follow-up read. Missing claims
still use the existing single-row conditional write, and a competing winner
must have the same ID. Counter floors, final authoritative validation, and
per-query dictionary refresh remain enabled. No migration marker or counter
heuristic replaces those checks. Conflicts fail initialization; reserving a
counter floor can leave a gap but must not publish a conflicting term claim.

This relies on the existing append-only claim contract. Concurrent modern
allocators reserve IDs before publishing claims; manual rewrites and old
writers inventing overlapping IDs during migration are not supported.
Migration temporarily holds both legacy and authoritative maps, so its memory
use remains linear and can exceed the old migration peak. First-time migration
still performs one conditional claim per missing term; startup gains apply to
already migrated terms and do not imply faster joins or per-query refresh.

MdbDictionaryInitTest.Startup128, Startup1024 and Startup8192 are opt-in: set
MDB_DICT_INIT_BENCHMARK=1 and run one case at a time through yb_build.sh. Each
case checks complete ID/term mappings, records one first plus five repeated
dictionary initializations, and separately records two fresh CLI processes.
InitStats counts legacy term claim attempts, not total storage or wire RPCs.
Controlled dictionary seeding is outside timing. First is not cold-cache;
two CLI observations do not establish tail latency or multi-machine scaling.


## Authoritative dictionary pages

MDB scans rdf_terms with strong 1024-row pages, following continuations across
tablets, including empty pages. The preceding operation retains its arena
while its continuation is copied; the session starts a new arena per page.
Only the application-owned previous/current pages are retained this way;
asynchronous client references and total process peak memory are not measured.
There is no strict page byte budget: long RDF terms can make a page large.
The complete dictionary still uses O(N) memory. Generic TableRange and the
legacy mirror scan retain their original defaults.

Row/null/identity/counter checks, atomic destination replacement on success,
and refresh after pinning the data view remain enabled. A counter reservation
can precede publication: an unchanged counter never justifies skipping refresh.
ScanStats reports rows including the counter, page read operations and serialized
response bytes; it excludes the separate counter read and metadata work, and
does not count wire RPCs. It describes the last scan, including a failed scan.

MdbDictionaryScanTest.Refresh128 and Refresh8192 are opt-in via
MDB_DICT_SCAN_BENCHMARK=1. Run each separately through yb_build.sh. They compare
complete bidirectional mappings after one first plus five repeated Restores.
Report dictionary-only, initialized API and fresh CLI timings separately.
The same scanner runs during initialization and migration validation, so
worker OPEN and CLI initialization can also change without worker reuse.


## Worker last-probe page reuse

An inner depth can reuse its existing decoded page only when the first fetch
completed that entire probe. The permutation and all three known term IDs
must match. Raw triples are rebound for every incoming row; result bag and
repeated-variable checks are unchanged. Complete empty probes can be reused.
Anchors, multi-page probes and an exact-size page needing a further EOF fetch
are not admitted. Changing keys replaces the single last-probe entry.

The page belongs to one fragment and its immutable child transaction snapshot;
it never survives a new OPEN/process/query. Restart and deadline checks remain
active. No second decoded buffer or result-sized cache is allocated. The
small key/counter state and one page fit the existing per-level conservative
reservation, with the same page, credit and memory admission limits. This is
not a total process RSS bound or a claim that peak memory was measured.

This is repeated-probe reuse, not batching different keys or reusing processes.
Unique keys may not benefit, and full-page/multi-page fanout may fall back.
Measure remote storage operation counts and complete end-to-end results for
all shapes, not just the shared-key case. The wire protocol, Gather accounting,
default join strategies and global/exchange execution remain unchanged.


## Worker leaf lookahead batches

The last triple can look ahead at up to eight raw parent-page rows. It rebinds
each candidate with the current prefix, groups identical known IDs/permutation,
and flushes their first pages together through DocDBScanCursor::NextBatches.
Emission keeps depth-first row order and rechecks every binding. Complete
first pages may serve duplicate keys within the window; partial pages transfer
their own cursor/continuation exactly once. Intermediate levels keep streaming.
A parent generation changes on new pages and on cached-page reopen, so reused
parent data cannot retain an earlier prefix's lookahead window.

Additional cursor/page storage is charged against memory_bytes minus the
unchanged FragmentBufferedReservation. Each slot reserves its object state,
8192 bytes plus page_rows*256 and variable_count*16 bytes. At most eight slots
are reserved. Fewer than two affordable slots disables lookahead and keeps the
original serial path; the original minimum memory remains valid. This is a
conservative buffered-state reservation, not a total RSS or allocator proof.
All probes use the same child snapshot/session, deadline and restart checks.

MDB_WORKER_IO_PROFILE=1 enables one stderr JSON record per successful fragment
before EOF: logical read operations, synchronous flush rounds, maximum queued
reads, prefetch hits, base and additional reservations. A flush is not a wire
RPC. Diagnostics are opt-in; the protobuf protocol and default join strategy
are unchanged. Process reuse and batching at intermediate levels remain work
for separate milestones.


## Worker completion boundary

Gather waits for a zero worker (or SSH transport) exit after a valid final
frame, before adopting its child transaction result or scheduling the next
task. Closing stdin ends the request side; nonblocking wait polls still honor
the original query deadline and cancellation. Errors retain the existing
kill/reap fallback. EOF alone no longer hides a failing process destructor.
Streaming callbacks may already have received rows before a late failure;
callers must discard failed query output, as the CLI already does.

Process/client pools are deferred. Current OPEN timing includes storage/client
initialization and snapshot setup, so it is not an estimate of reusable startup
cost. Persistent workers would need explicit snapshot/state reset and failure
isolation; this change adds neither a service nor a new protocol/configuration.


## Ready-frame Gather

After OPEN, Gather polls all credited worker outputs and consumes the next
complete ready frame, rotating preference among ready streams. Each stream's
incremental reader preserves a partial four-byte header or body without
blocking other streams. It accepts at most the existing 1 MiB frame payload;
the payload allocation is released after parsing. At most 16 active streams
can retain one partial payload each (16 MiB payload bound, plus one parsed
response and allocator/metadata overhead). This transport bound is separate
from coordinator_memory_bytes, which continues to limit retained operator
input rather than process RSS. No result-sized queue or extra credit is added.

Per-stream sequence/schema/identity, snapshot and final zero-exit checks remain.
Unordered query rows may interleave differently; ORDER BY still uses the global
finalizer. EOF cleanup and the sink remain synchronous. OPEN acknowledgements now use
the same ready-frame loop (see the two-input follow-up).
This is receive-side progress under uneven workers, not parallel exchange
consumers, external global sorting or a persistent worker service.


## Exchange stage profile

Profiles add exchange_total_us, exchange_setup_us, exchange_left_input_us,
exchange_right_input_us, exchange_partition_us, exchange_consume_us,
exchange_sink_us and exchange_cleanup_us. Input scopes include their partition
callbacks; the combined partition counter covers hashing/validation and spill
write calls. Consume covers local spill reads/hash join and its output sink.
Sink is nested in consume. Do not add nested counters to their parent phases.
Cleanup covers explicit cleanup and destructor fallback, including error paths.
Total starts after configuration/entry checks and includes metadata setup,
sequential inputs, consumption and cleanup; non-exchange queries report zeros.
These are elapsed wall times, not CPU time, physical disk latency or wire RPCs.

The three 4096-size local fixtures attribute most exchange time to sequential
inputs (see the validation checkpoint). This supports evaluating interleaved
input scheduling under one parent snapshot and one aggregate worker limit.
It does not establish the benefit of two producer threads, a process pool or
parallel local join consumers. Execution order/policies and budgets are unchanged.


## Interleaved two-input follow-up

Opt-in Hash/Broadcast exchange now supplies both inputs through one Gather
event loop. It alternates task admission across inputs and skips exhausted or
empty inputs, with one aggregate max_workers cap. All tasks use one exported
child read view. Storage identity/coverage and both physical inputs are checked
before launch; fragment IDs are unique across the pair, and each stream uses
its own projection width. Per-stream credit, frame bounds, schema/envelope and
zero-exit completion checks remain. Both inputs must succeed before join output.

OPEN acknowledgements and data frames share the bounded incremental reader.
Refilling a slot no longer blocks on that worker's OPEN; other active streams
can progress. remote_open_wait_us counts pooled waits ending in an OPEN frame;
remote_pull_wait_us counts waits ending in data (and failed waits). These are
disjoint coordinator wait samples, not individual worker startup lifetimes.
Their attribution differs from the prior synchronous OPEN implementation.

exchange_input_us is the whole, non-overlapping input phase in both APIs.
The retained serial ExchangeRows API also records left_input/right_input_us;
those legacy fields are zero for the interleaved adapter path. The adapter
increments exchange_interleaved_operators. Use setup+input+consume+cleanup
against exchange_total_us; partition remains nested in input, sink in consume.
Do not infer that zero legacy side counters mean no input work.

No new threads, CLI flag, process pool, extra credit, default exchange selection
or larger resource budget is introduced. Plan-pair metadata replaces sequential
child-plan metadata; buffered frames and worker reservations still depend on
the aggregate active count, not twice that count. EOF cleanup, sinks and local
join consumers remain synchronous. Current small local comparisons have mixed
timing results; independent progress is validated, not universal speedup.


## Finite global ORDER BY windows (2026-10-29)

For a plain, non-DISTINCT/non-aggregate BGP SELECT with bound-variable ORDER BY
and finite LIMIT, the coordinator can retain only the best LIMIT+OFFSET full
typed bindings. A worst-first heap uses the existing compare_for_order and
ASC/DESC key sequence. Hidden variables and duplicate rows remain in bindings.
The original ORDER BY and slice run afterward, so OFFSET applies exactly once.
Equal-key order remains unspecified, as in the ordinary sort.

Eligibility excludes groups, HAVING, SELECT expressions, unbound sort keys and
overflowing windows. LIMIT 0 uses a zero-row window but still validates the full
input. The heap is selected only if K+1 conservative row slots fit the existing
coordinator budget; otherwise the original full-materialization admission stays
in place. Row charge is global_row_reservation plus twice the typed row payload
and twice the vector descriptor, including capacity slack. One incoming-candidate
slot is charged while the K-row heap is full. This is retained-operator accounting,
not process RSS: dictionaries, parsed plans, client/RPC and transport are separate.

Every fragment still drains and must pass its final child-result/zero-exit
checks, even after the heap is full or LIMIT is zero. No worker-local pruning,
early output or cancellation bypass is introduced. global_input_rows counts all
validated rows; global_topk_operators/capacity/retained_rows identify the path,
and global_reserved_bytes reports peak conservative admission. Hash/Broadcast
exchange can feed the same collector. DISTINCT/grouped and unbounded global
operations still have their original full-input limit; external sort is pending.


## Projected DISTINCT admission (2026-10-29)

Eligible plain BGP `SELECT DISTINCT` queries retain one full typed representative
per projected ObjectId tuple before the existing global finalizer. Every selected
variable must be bound, with no SELECT expressions, GROUP BY, HAVING or aggregates;
ORDER BY, if present, may only reference projected variables. Hidden sort keys,
unbound projection and all other envelopes keep their previous admission path.
Tuple identity uses the same ObjectId vector ordering as `DistinctHash`, not RDF
numeric value equivalence. ORDER BY and LIMIT/OFFSET still run exactly once in
the original modifier tree; neither LIMIT nor LIMIT 0 stops input validation.
Unordered output order, unordered slices and equal-key ordering remain unspecified.

The collector charges the unchanged downstream reservation plus map/key/full-row
storage and conservative capacity slack. One incoming slot is always admitted;
a new unique tuple requires another retained slot. Duplicate input does not add
retained slots. Too many unique tuples still fail within the configured budget;
there is no spill, truncation or fallback after remote work starts. The extra
index charge can reach admission earlier on high-uniqueness inputs than the
former all-row collector at the same nominal budget. Every input's typed
dictionary identity, all child results and successful worker exits remain
mandatory before global output. `global_distinct_operators` and
`global_distinct_retained_rows` identify this path. `global_input_rows` counts all
admitted input rows, and `global_reserved_bytes` is a conservative logical peak,
excluding dictionaries, client infrastructure and transport buffers; it is not RSS.

Release validation: 16 exact GTests (7 new, 9 related compatibility) passed using
one wrapper invocation per case. A pre-change 512-row/two-unique-value query failed
under 10,000 bytes after 7 rows/9,968 reserved bytes. The same query now succeeds
through workers, Hash and Broadcast. Growth from 64 to 256 to 1,024 input rows keeps
two representatives and 4,992 reserved bytes. Tests cover full projected
tuples, variable order, typed IRI/numeric lexical/datatype/language/blank identity,
empty/zero/offset output, exact/one-byte-short budgets, hidden/unbound/aggregate
envelopes, unique-cardinality exhaustion, late worker failure and recovery.
A fresh-import CLI test checks typed JSON/schema and failed-output discard.
Conditions: one WSL host, two logical CPUs, three in-process tservers/RF3 and four
tablets per index. This is not a latency, RSS, physical multi-host or SOTA result.
No Debug/ASan run; historical allocator root cause remains unconfirmed.
The first two cases passed before a test-only correction: an unbound projection
must use typed callbacks because the legacy text callback cannot represent an
empty solution. Its initial failing run is preserved, not counted as passed.
The corrected case and remaining thirteen cases passed in the final run; the
first two unchanged cases were not rerun. Runtime code did not change between
these runs.


## Incremental global counts (2026-10-29)

Plain BGP SELECTs with only direct non-distinct `COUNT(*)` or `COUNT(variable)`
aggregates now retain typed group-key tuples and uint64 counters instead of all
input bindings. Group keys use the same ObjectId identity as the original
internal group sort/aggregation, including unbound keys. COUNT of an unbound
variable is zero. An empty implicit aggregate returns one zero-count binding;
an empty explicit group returns no bindings. Count increments reject values
beyond INT64_MAX before mutation, matching the signed result representation.

After every input dictionary identity, child result and worker zero exit has
succeeded, finalized group keys/count aliases replace the count/group tree.
The existing SELECT projection, ORDER BY, DISTINCT and LIMIT/OFFSET then run once;
hidden group ordering keys remain available and LIMIT 0 still drains workers.
HAVING, COUNT(DISTINCT ...), numeric aggregates, computed SELECT expressions,
GROUP BY expressions and non-count grouping keep their previous paths. Only
the validated optional ORDER BY / optional GROUP BY / BGP envelope is rewritten.
There is no worker-local partial aggregation, network or storage format change.

Admission charges conservative map/key/counter, incoming typed row/key, finalized
Values/order/distinct storage and fixed-size generated count terms per retained
group. New groups require an additional retained slot; the implicit empty group
is admitted before gathering. Excess group cardinality fails without output or
post-start fallback. `global_count_operators` and `global_count_groups` identify
this path; `global_input_rows` still counts the complete admitted multiset.
`global_reserved_bytes` reports peak logical reservation, excluding shared
dictionaries, client/RPC infrastructure and transport buffers; it is not RSS.
High group cardinality can reach the same nominal budget earlier because of map
overhead. Unordered output/slices and equal ORDER BY keys have no stable order.

Release evidence: 19 exact GTests (9 new, 10 related compatibility), one wrapper
invocation per case. Pre-change, a 512-row/two-group query failed after five rows
and 9,760 reserved bytes under 10,000 bytes; workers, Hash and Broadcast now return
the expected complete counts under the same limit. Growth from 64 to 256 to 1,024
inputs retains two groups and 5,952 reserved bytes while exact counts grow.
Tests check typed key identity/column order, multiple keys/count aliases, bound
and missing arguments, empty groups, hidden ordering keys, DISTINCT over counts,
exact/one-byte-short budgets, too many groups, excluded envelopes, signed-count
overflow, late nonzero worker exits (also LIMIT 0), cancellation/timeout at output,
recovery, and fresh-import CLI JSON/schema/pagination with failed-output discard.
Conditions: one WSL host, two logical CPUs, three in-process tservers/RF3, four
tablets per index; CAPI page1/batch2, CLI page16/batch128. No new latency, peak RSS,
Debug/ASan, physical multi-host or SOTA result. Historical allocator root cause
and remote cleanup limitations remain. General HAVING/value aggregation and
external sorting/aggregation are not implemented by this milestone.


## Incremental COUNT HAVING (2026-10-29)

The bounded global count path now accepts the existing restricted HAVING
predicates when every aggregate in SELECT and HAVING is a direct non-distinct
COUNT(*) or COUNT(variable). There must be at least one count. GROUP BY remains
variable-only and SELECT computed expressions stay on their previous path.
HAVING retains its existing pre-launch validation (including no SELECT alias
visibility, depth/node/private-aggregate limits and the 128-variable bound).
COUNT DISTINCT, SUM/AVG/MIN/MAX and unsupported expressions retain original
all-input admission or the existing pre-launch capability fallback.

Each HAVING count occurrence receives a private counter slot. Planning clones
the bounded predicate and reserves identities without mutating the query context
or input tree. Every input row is still validated and counted; all child results
and successful worker exits are required even when filtering removes every group
or LIMIT is zero. Only then are private variables allocated and aggregate leaves
rewritten to finalized count variables. The existing expression evaluator filters
final group bindings before the original ORDER BY/projection DISTINCT/slice.
There is no worker-local HAVING filter and no new boolean/error evaluator.
Implicit empty groups produce zero counts before filtering; explicit empty
groups produce no rows. Missing count arguments, unbound group keys and boolean
error truth tables retain their existing semantics. Private counters never
change public result columns. A count/group-only query without HAVING uses the
same finalization helper without private variables or an added filter.

Additional private counters are included in existing per-group/candidate
reservation, binding-slot and HAVING-node charges. `global_count_having_operators`
and `global_count_having_hidden_counts` identify this path; `global_input_rows`
still counts every admitted input and `global_count_groups` is pre-filter group
cardinality. Neither the hidden counters nor HAVING reduce the group admission
limit early. Too many groups still fail; this is not an external aggregation.

Release evidence: 19 exact GTests (7 new, 12 relevant compatibility), one wrapper
invocation per case. Pre-change, 512 input rows/two groups with HAVING reached
9,440 reserved bytes after five input rows and failed at a 10,000-byte limit.
The same complete-count filter now succeeds through workers, Hash and Broadcast.
Growth from 64 to 256 to 1,024 input rows keeps two groups, one private counter
per group and 6,960 peak logical reserved bytes. Comparisons check complete
ordered results and independent expected counts against the original coordinator.
New tests cover multiple hidden counts, missing variables, boolean errors,
hidden-only count projection, SELECT alias rejection before launch, empty groups,
DISTINCT/slice ordering, exact/one-byte-short budgets, excluded aggregates,
all-filtered/zero-limit late failures, cancellation/timeout and recovery.
Fresh-import CLI JSON checks typed counts, public schema/private-column exclusion
and failed-output discard. Existing count/DISTINCT/HAVING/expression/numeric and
exchange cases pass. The previous count exclusion test now uses COUNT DISTINCT
in HAVING to preserve its original all-row admission purpose.

Conditions: one WSL host, two logical CPUs, three in-process tservers/RF3 and four
tablets per index. CAPI page1/batch2; CLI page16/batch128. Logical reservation
excludes shared dictionaries, client/RPC infrastructure and transport buffers;
it is not RSS or a latency result. No new physical multi-host, SOTA, Debug or
ASan acceptance; historical allocator root cause and remote cleanup limits remain.
No wire/storage format, default exchange/Hash/Leapfrog policy or worker-local
aggregation changes. High group cardinality can still exhaust the same budget.
The first implementation used an ordinary OpFilter and failed the bare
HAVING(COUNT(*)) regression because that path uses a different boolean converter.
It was corrected to preserve OpHaving over the finalized Values, thereby using
the original HAVING RDF conversion. The first run (one pass, one failure) is
preserved separately and is not included in the final 19 passes. Runtime code
changed for this correction, so all 19 cases were rerun on the corrected binary.


## Bounded variable DISTINCT counts (2026-10-29)

The incremental count path now also accepts direct COUNT(DISTINCT variable) in
SELECT and the existing restricted HAVING predicates. Each group owns a typed
ObjectId set per distinct bound argument column. Repeated counts of the same
argument, including private HAVING counters, share that set within the group.
Different groups and different argument columns never share sets. Unbound
arguments return zero without allocating a set; null values are not inserted.
Identity matches AggCountDistinct rather than RDF numeric value equivalence.
Ordinary counts still count the complete multiset alongside the distinct sets.

COUNT(DISTINCT *) remains on its original full-solution-tuple path, including
queries that mix it with variable DISTINCT counts. Computed SELECT expressions,
numeric aggregates and unsupported envelopes retain previous behavior. The
existing final HAVING node, alias scope, ORDER/DISTINCT/slice and complete child
validation remain unchanged. No early success for LIMIT 0 or fully filtered output.

Empty-set metadata adds 128 conservative bytes per configured argument set to
each group slot. Every unique (group, argument, ObjectId) entry charges another
128 bytes. Group/candidate slots and all entries share coordinator_memory_bytes;
all new group and set nodes are admitted together before allocation. Duplicates
do not add entries. Excess groups or unique values fail explicitly, without
truncation, unbudgeted fallback or spill. Reservations are logical conservative
estimates, excluding shared dictionaries, client/RPC and transport buffers, not
RSS. High uniqueness may exhaust the same nominal budget earlier due to indexes.

global_count_distinct_operators marks queries with variable DISTINCT counts
(including only unbound arguments). global_count_distinct_sets is configured
bound-argument sets per group; global_count_distinct_entries sums actual entries
across all groups and argument sets. Repeated output/private counters do not
multiply the entry charge. Empty implicit/explicit group semantics and signed
count bounds are retained; output cardinalities use the existing integer packer.

Release validation: 19 exact GTests (8 new, 11 related compatibility), one wrapper
invocation per case. A pre-change 512-input query retaining two distinct terms
failed after four rows and 9,472 reserved bytes under a 10,000-byte budget. It now
succeeds through workers, Hash and Broadcast with repeated SELECT/HAVING counts
sharing one set. Input growth 64/256/1,024 keeps two groups, one set per group,
two entries total and 7,600 peak logical bytes. Exact/one-byte-short combined
budgets, high uniqueness, independent arguments/groups, missing/empty inputs,
typed lexical/datatype/language/blank scope identity, final modifiers, late errors,
cancellation/recovery and fresh-import typed CLI/schema/failure discard pass.
Full-tuple exclusion and relevant old all-input budget tests now explicitly use
COUNT(DISTINCT *); their assertions are retained with the tuple reservation charge.

One WSL host, two logical CPUs, three in-process tservers/RF3, four tablets per
index; CAPI page1/batch2 and CLI page16/batch128. No new RSS, latency, physical
multi-host, SOTA, Debug or ASan acceptance. Historical allocator root cause and
remote cleanup limits remain. No storage/wire, default algorithm or CLI changes.


## Finite unordered windows (2026-10-29)

Plain BGP SELECTs with finite LIMIT/OFFSET and no ORDER BY, DISTINCT, grouping,
HAVING, aggregate or SELECT expression may now keep only the first K complete
arrivals, where K=LIMIT+OFFSET (K=0 for LIMIT 0). The existing bounded candidate
collector has an explicit empty-sort-key prefix path that preserves duplicates
and arrival order, without heap operations. The original Slice still applies
OFFSET once after all child completion checks. All subsequent dictionary rows,
child results and worker zero exits are validated even when the prefix is full
or K is zero; this change does not stop scans or network input early.

The same conservative K+1-slot admission as ordered Top-K must fit the existing
coordinator budget; overflowing/too-large windows and unbounded OFFSET retain
the original all-input admission path. Existing DISTINCT/count/ordered paths
remain separate. global_slice_operators/capacity/retained_rows mark this path;
ordered Top-K metrics retain their previous meaning. There are no new flags,
wire/storage changes, default join/exchange changes or worker-local limits.

16 exact Release GTests passed (6 new, 10 related compatibility), each via the
wrapper separately. A pre-change 512-input LIMIT5 OFFSET7 query failed under
20,000 bytes; workers, Hash and Broadcast now return five valid complete rows.
The test checks full row membership/multiplicity, not arbitrary unordered order.
A max_workers=1 complete-input comparison checks the exact received window and
one application of OFFSET. Duplicate projected rows remain duplicated. Empty,
zero, overflow, exact/one-byte-short budgets, late worker exits, cancellation,
recovery and a fresh-import typed CLI/failure-output-discard gate pass.
Input 64/256/1,024 retains twelve bindings and 19,552 peak logical reserved
bytes. This excludes shared dictionaries/client/transport buffers and is not RSS
or a latency improvement. Unordered output and slices are not stable across
worker schedules; this does not add an ordering guarantee.

One WSL host, two logical CPUs, three in-process tservers/RF3, four tablets per
index; CAPI page1/batch2, CLI page16/batch128. No new physical multi-host, SOTA,
Debug or ASan acceptance. Historical allocator/remote cleanup limitations remain.
Unbounded global sorts, numeric/external aggregation and worker-to-worker
consumers remain outside this milestone. The old all-input exclusion case now
uses unbounded OFFSET rather than finite LIMIT to retain its budget assertion.


## Incremental direct MIN/MAX (2026-10-29)

The existing grouped state carrier accepts direct MIN(variable)/MAX(variable)
SELECT aggregates without HAVING, optionally mixed with supported COUNTs and
variable COUNT DISTINCT. It retains one winning typed ObjectId per extremum slot
and uses the original compare_for_order/null handling. DISTINCT on MIN/MAX is
idempotent and does not allocate a set. Equal comparison keys retain the first
arrival; ties do not acquire a stable representative across worker schedules.
Missing arguments and empty groups retain the existing unbound result behavior.
Values are passed unchanged to the final projection/order/distinct/slice, after
all inputs/dictionaries/child results and worker exits have succeeded.

Each group reserves the previous conservative downstream state plus extrema
vector/candidate capacity. Existing count and distinct-set admission still share
the same budget; too many groups fail without truncation or fallback after launch.
global_extrema_operators and global_extrema_slots identify the path; the existing
global_count_groups/operations metrics also cover the reused grouped carrier.
SUM/AVG, computed SELECT extensions, full-tuple distinct counts, and any HAVING
with selected extrema retain their previous paths. No changes to storage/wire,
flags, default join/exchange strategy, worker-local aggregation or boolean logic.

Release evidence: 16 exact GTests (6 new, 10 compatibility), one wrapper call per
case. Before the change, 512-input MIN/MAX plus COUNT query failed at 10,000 bytes
after three rows/8,832 reserved bytes; workers/Hash/Broadcast now return complete
correct results under the same limit. Input 64/256/1,024 retains one group/two
extrema slots and 6,352 peak logical bytes. Numeric winner identity across
integer/decimal/double, missing/empty groups, grouped final modifiers, exact and
one-byte-short budgets, excluded paths, late failure/LIMIT 0, cancellation and
recovery pass. A fresh-import CLI checks typed IRI winners/schema/failure discard.
Existing numeric, count, HAVING, DISTINCT, windows and exchange regressions pass.

Conditions: one WSL host, two logical CPUs, three in-process tservers/RF3, four
tablets per index; CAPI page1/batch2, CLI page16/batch128. Logical reservations
exclude shared dictionaries/client/RPC/transport buffers, not RSS or measured
latency. No new Debug/ASan, physical multi-host or SOTA evidence. Historical
allocator root cause and remote cleanup limitations remain unresolved.


## Incremental extrema in HAVING (2026-10-29)

The existing bounded HAVING predicate grammar now rewrites direct variable
MIN/MAX leaves to private grouped slots, alongside supported COUNT variants.
Selected extrema can also use grouped state with COUNT-only HAVING. Both use
the same aggregate binding and final rewrite; no new predicate or literal
grammar is enabled. Typed winners, missing/empty groups, original HAVING RDF
boolean evaluation, alias scope and final ORDER/DISTINCT/projection/Slice remain.
Planning does not register private variables. Complete input/dictionary/child
success and worker zero exit are required before finalization, including filtered
or LIMIT 0 output. Private extrema are charged with all other grouped state.

SUM/AVG, computed SELECT and full-tuple COUNT DISTINCT retain existing paths.
The legacy global_count_having_hidden_counts metric counts all private aggregate
slots on this shared carrier; global_extrema_slots includes selected and hidden
extrema. There is no worker-local aggregation or wire/storage/default change.

Release: 17 exact tests (7 new, 10 compatibility), one yb_build.sh call per case.
The original 512-input query exhausted 20,000 bytes after four rows (18,688
reserved); workers/Hash/Broadcast now return the complete expected result.
Hidden-extrema growth 64/256/1,024 inputs holds one group/two hidden extrema and
7,056 peak logical reserved bytes. Tests cover typed numeric winners,
boolean/null/empty semantics, alias scope, final modifiers, exact/one-byte-short
budgets, high group counts, unsupported paths, late failure/cancel/timeout and
recovery. Fresh-import CLI verifies public schema/types and failed output discard.
The former direct-extrema HAVING exclusion uses SUM HAVING to preserve its
full-input-budget purpose. Equal comparison keys still select first arrival.

One WSL host/two logical CPUs, three in-process tservers/RF3, four tablets/index;
CAPI page1/batch2, CLI page16/batch128. Logical reservation excludes dictionaries,
clients and transport; no RSS/latency, physical multi-host, SOTA or new ASan claim.
Item 11 remains incomplete: external/unbounded operations, SUM/AVG and actual
worker-to-worker consumers are separate work. Existing allocator/cleanup limits
and paused persistent pools remain.


## Optional worker boundary timing (2026-10-29)

Set MDB_WORKER_PHASE_PROFILE=1 in the worker environment to emit one
MDB_WORKER_PHASE JSON record to stderr before terminal EOF. Unset/other values
disable clock reads and output. SSH does not automatically forward this variable.
OPEN covers validation/buffers plus nested clock/client creation, six table opens
and child transaction/session preparation. NEXT includes nested scan_api time
(cursor preparation/flush/response decode together), binding/projection and child
finalization. Its remainder is not pure join CPU. writes_before_eof includes
serialization/pipe backpressure for earlier responses, excluding the final EOF
write and shutdown; abrupt kill/disconnect need not produce a record. A success
field describes handler status and never substitutes for Gather's exit check.

No wire, strategy, credit, snapshot, prefetch capacity or memory budget changes.
Only fixed-size worker diagnostic metadata is added. Nine exact Release tests
pass (three new plus six relevant regressions), including diagnostic on/off bag
and read equivalence in workers/Hash/Broadcast, late nonzero exit, minimum budget,
snapshot, cancellation and recovery with profiling enabled. A same-binary 512
subject test alternates off/on, checks all 1,024 output values on every query,
and reports first plus five repeated samples per setting. This is diagnostic
overhead evidence on a small local workload, not a speedup or RSS guarantee.


## Strategy entry guard (2026-10-29)

Stage12QueryOrderAndExplicitStrategiesKeepTypedBag checks a two-triple shared-key
graph with 128 subjects and 256 typed two-column results. Both textual triple
orders run with local Index/Hash/Auto and explicit workers/Hash/Broadcast exchange;
all twelve cells retain complete bags, variable order and no coordinator fallback.
This is a correctness/selection observation, not a repeated timing benchmark.
Local JoinPolicy chooses a local join; exchange is a separate explicit worker
option. FragmentPlan currently collects triples in logical-tree order; the local
greedy optimizer is not a distributed exchange/locality cost selector. Defaults
remain Index and exchange None. Multi-scale/selectivity/locality evidence is
required before any automatic distributed strategy rule is introduced.


## Two-scale explicit strategy matrix (2026-10-29)

MdbStrategyMatrixTest runs unique/shared/fanout at 128/512 outer subjects, both
textual triple orders and coordinator Index/worker Index/Hash/Broadcast paths.
An independent typed two-column multiset oracle includes all duplicates. Six
exact Release tests pass: 192 complete observations, 48 initial plus 144 repeats
(three per cell). Cells rotate between rounds; validation is outside timing.
Page16/batch128, three worker limit, unchanged budgets/defaults and local RF3
storage apply. Raw read/flush counts are not wire RPC counts. Input cardinalities
recorded as seed truth are not planner estimates; distributed estimate fields
are not populated by the inspected FragmentPlan::Build. Missing estimates do
not mean zero rows. Hash build estimates do not describe distributed exchange.

These small single-host samples do not justify always scanning the smaller side
first or always choosing the fewest-read exchange. Startup/task count, indexed
probe reuse and repeated pages affect cost. Strategy changes require query-view
statistics, distinct join-key estimates and budget/locality evidence; absent or
incomplete statistics should retain existing behavior. No runtime/default change
is made by this matrix. Raw samples and three-repeat ranges are required when
interpreting medians; no SOTA, physical scaling, RSS or significance claim.


## Historical predicate statistics advisory (2026-10-29)

Worker profiles expose distributed_join_advisory for the accepted physical plan.
Only two fixed-predicate patterns with distinct variable subject/object endpoints
and exactly one shared variable are eligible. The query context's immutable
ANALYZE statistics pointer supplies version/time and predicate counts. Complete
maps distinguish a historical zero from missing/incomplete statistics (null).
Other shapes, absent/version-zero or partial statistics retain explicit reasons.
No predicate IDs, RDF text or unbounded maps are serialized.

The ANALYZE scan predates the query data view. Local writes invalidate the model
cache, but external writers are not proven absent. The profile always marks
freshness_verified=false, join_key_statistics_available=false, applied=false,
and recommends retaining explicit order/mode. Counts are historical observations,
not a current-view join cardinality or a cost-based automatic selection. No
extra scan/RPC, ordering, wire, resource limit or default algorithm change occurs.

Nine exact Release cases pass: guards for absent/partial/zero/unsupported inputs;
24 real ANALYZE-backed complete typed query observations across six graphs,
both orders and worker/Hash paths; local-write invalidation with immutable older
statistics; and relevant strategy, snapshot, budget, aggregate, fallback and CLI
regressions. The profile remains null for local/non-accepted worker plans. No
new latency/RSS, physical multi-host or SOTA claim is made.
