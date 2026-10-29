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
