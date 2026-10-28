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
