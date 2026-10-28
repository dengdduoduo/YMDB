# MDB validation checkpoint - 2026-10-28

Peer implementation base: eace9d83ac9e98a8f45ba95e0c9ab6ecd5aa8d3f.
This branch includes graph result/VALUES/MINUS/path/ASK/CONSTRUCT corrections,
typed result callbacks, persistent dictionary recovery and multi-writer checks,
explicit client/transaction lifetime handling, dictionary startup/page work,
and bounded worker probe reuse/lookahead. These are research prototype changes.

## Latest actual validation

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
is still unconfirmed. Full natural worker-exit protocol remains incomplete.
