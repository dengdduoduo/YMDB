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
