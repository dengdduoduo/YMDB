# MDB validation checkpoint - 2026-10-28

Peer implementation base: eace9d83ac9e98a8f45ba95e0c9ab6ecd5aa8d3f.
This branch includes graph result/VALUES/MINUS/path/ASK/CONSTRUCT corrections,
typed result callbacks, persistent dictionary recovery and multi-writer checks,
explicit client/transaction lifetime handling, dictionary startup/page work,
and bounded worker probe reuse/lookahead. These are research prototype changes.

## Leaf-batching validation

Release/clang21 built through yb_build.sh. Latest batch: 27 distinct GTests passed,
including seven new batch gates and existing probe/snapshot/budget/cancel/exit
regressions. Three graphs, four modes, one first plus five repeated API calls
and two fresh coordinator CLIs per graph were checked before/after: 156
complete typed result/schema observations (144 API, 12 fresh CLI).

One WSL host, two logical CPUs, three in-process tservers/RF3, four tablets per
RDF index, graph page16 and worker batch128. First is not cold-cache.

| Shape / three workers | Before ms | After ms | Before / after flushes |
|---|---:|---:|---|
| unique | 2188.66 | 978.56 | 4354 / 772 |
| shared | 372.53 | 377.21 | 262 / 262 |
| fanout | 2307.83 | 1124.29 | 4287 / 771 |

## Reproduce the fixtures

Generate inputs with src/yb/mdb/test_data/gen_stage9_baseline.py and set
MDB_STAGE9_DATASETS to its output directory. Set MDB_STAGE9_REPEATS=5 and
MDB_WORKER_IO_PROFILE=1. Run MdbStage9BaselineTest.Unique4096, Shared4096 and
Fanout4096 individually via yb_build.sh --cxx-test mdb_fragment_integration_test
--gtest_filter <one-exact-case>. Also run MdbWorkerBatchTest cases individually.
Use the matching Release build root; build the target first. See PERFORMANCE.md.

## Limits

The current optimization batches only last-level first pages, with at most
eight budgeted slots. It does not establish multi-machine scaling, full SPARQL
coverage, tail latency, SOTA ranking or production readiness. Process reuse and
parallel/global execution extensions remain separate work. No latest Debug,
ASan or peak-RSS validation was performed. A historical mismatched-size-class
shutdown incident has not recurred in the latest gates but its exact root cause
is still unconfirmed. Normal EOF now waits for a zero local worker or SSH transport exit. Failure
cleanup still uses local kill/reap; full remote failure lifecycle under real
multi-machine faults remains unverified.


## Worker completion follow-up

Ten related Release tests passed: four new MdbWorkerExitTest gates (delayed
normal exit, nonzero/signal after EOF, deadline, cancellation) and existing
worker-failure/recovery, shared snapshot, authenticated SSH/host-key rejection,
CLI output staging, repeated API/fresh CLI shutdown, and Shared4096 fixtures.
Before the fix, the first two gates reproduced premature kill and a false
successful query after a failing process exit. EOF completion now checks exit
status before adopting the child result; deadline/cancellation still apply.
Consumers must discard already streamed rows when the final query status fails.

Shared4096 recheck validated all 26 typed observations. Three-worker repeated
API median was 372.93 ms (previous batch 377.21 ms); median cleanup was 12.36 ms.
This is a related no-regression check, not a new speedup claim or a tail-latency
measurement. Persistent process/client pools are deferred pending measured
benefit and explicit state/snapshot reset design.


## Ready-frame Gather follow-up

Sixteen related Release tests passed: five new frame/progress tests and eleven
existing cross-tablet, global order/distinct/slice, exchange/composite join,
snapshot, failure, authenticated SSH, CLI staging, probe/batch cancellation,
late-exit and Shared4096 fixtures. A real-worker relay gates fragment zero on
output from another fragment: unchanged code hit the gate timeout; new code
completed without timeout for zero bytes, a partial header and a partial body.
The full bag matched in every successful gated run. Pending-frame cancellation
and expiry failed explicitly and recovered in a fresh query. Frame tests check
split/back-to-back input, zero/oversized lengths, malformed protobuf and EOF.

Shared4096 rechecked 26 complete typed observations; three-worker median was
420.11 ms (previous completion-boundary batch 372.93 ms).
All modes were slower in this sequential recheck, including the untouched
coordinator control (266.37 to 328.16 ms). This does not isolate a regression
caused by receive scheduling, or establish a no-regression/speedup claim. The gate proves
independent progress, not a network benchmark. OPEN and EOF cleanup remain
synchronous; full distributed exchange/global extensions remain future work.


## Exchange profiling follow-up

Eight related Release tests passed, including the new stage-timing nesting,
slow sink, quota failure, reset and recovery gate. Existing hot-key spill,
quota/cancellation, shared-snapshot and CLI cases passed. Unique4096, Shared4096
and Fanout4096 checked 78 complete typed/schema observations (72 API, six fresh
coordinator CLIs). Every profile passed nesting and non-exchange-zero checks.
Input hashes match the earlier dataset archive. Execution strategy is unchanged.

Five repeated exchange API samples per shape, median wall milliseconds:

| Shape | Exchange total | Left input | Right input | Partition (nested) | Consume | Inputs share |
|---|---:|---:|---:|---:|---:|---:|
|unique|628.05|310.09|289.81|6.55|13.97|97.6%|
|shared|520.93|289.48|178.72|4.89|9.75|97.8%|
|fanout|630.50|329.52|257.09|4.76|48.66|92.8%|

Percentages are medians of per-sample input/total ratios; phase medians need not
sum exactly. Input includes partition; consume includes sink. These results
identify sequential input production as the next candidate, without a speedup
claim. No concurrent input implementation, default exchange selection, new
memory budget or external/global consumer is introduced by this measurement.


## Interleaved two-input follow-up

Nineteen distinct related Release GTests passed after the OPEN refinement.
Seven new cases include four pair/schema/coverage/snapshot/failure gates and
three comparisons. The first implementation passed correctness but regressed
local timing; its synchronous OPEN refills were not published. OPEN now uses
ready-frame dispatch, with a gate proving the right input progresses while the
left has a partial OPEN or PULL frame. All relevant cases were rerun after this
source change. The old round and source snapshots remain separate evidence.

Final comparison: 72 complete storage-ID bag observations, n=512, same binary,
two CPU affinity, workers=1/3, serial/interleaved order alternated, first plus
five repeats. The serial path uses the same refined single-input Gather.
Read-context creation, import, parsing and dictionary work are outside this
boundary. Change is interleaved/serial minus one; positive means slower.

| Shape | Max workers | Serial ms | Interleaved ms | Change |
|---|---:|---:|---:|---:|
|unique|1|628.20|622.32|-0.9%|
|unique|3|437.76|449.73|+2.7%|
|shared|1|629.40|607.37|-3.5%|
|shared|3|407.55|378.15|-7.2%|
|fanout|1|677.92|634.25|-6.4%|
|fanout|3|482.55|503.47|+4.3%|

Shared4096 additionally checked 26 typed/schema API and fresh coordinator CLI
observations. Final gates preserve mixed widths, Hash/Broadcast bags, shared
snapshot vs new writes, aggregate worker/memory limits, prelaunch rejection,
empty-side completion, quota/cancel/timeout/late-exit cleanup and recovery,
global modifiers, SSH and CLI behavior. This is one WSL host, not multi-machine
scaling or a SOTA/tail-latency result. Timing gains are mixed; no general speedup
or statistical no-regression claim. Profile readers should use exchange_input_us;
legacy serial side timers are zero on the interleaved adapter path.


## Finite global ORDER BY window follow-up (2026-10-29)

Sixteen distinct related Release GTests passed (seven new, nine compatibility).
A pre-change query requesting LIMIT 5 OFFSET 7 failed under a 20,000-byte
coordinator result budget because it retained all 512 rows. The scoped heap
path now returns the full-sort reference result in worker/Hash/Broadcast modes.
Three growth pairs retain a fixed window while the full input grows:

| Input rows | Retained rows | Top-K peak reservation bytes | Full-sort reservation bytes |
|---|---:|---:|---:|
|64|12|19968|92160|
|256|12|19968|368640|
|1024|12|19968|1474560|

These are logical reservations, not measured RSS or a latency speedup study.
The full-sort oracle omits LIMIT, and its ordered result is sliced independently.
Tests cover hidden keys, ASC/DESC, projected duplicates, typed numeric/IRI/lang/
blank identities and output variable order, exact budget/one-byte-short, zero
LIMIT with large OFFSET, overflow/large-window fallback, excluded DISTINCT and
groups, late worker exit, cancellation, recovery and real CLI JSON/failed-output
discard. Related global aggregates, modifiers, budgets and exchange tests pass.
All input and child-result checks still run before final output. Existing
OrderByWithLimit placeholder behavior is not used by this collector; the normal
finalizer remains responsible for ordering and slicing retained candidates.


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
