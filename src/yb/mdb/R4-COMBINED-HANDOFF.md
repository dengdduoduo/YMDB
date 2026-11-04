# R4: combined session and graph-policy acceptance

Prepared 2026-11-04. Product base:
`e7584c21bfbba13057d6aa38159d7d3645f3df85` on shared `ymdb-ckr`.
This delivery adds measurement/test/documentation only. Compiled query execution,
storage, protocol, Auto selection, default Old and default-off worker reuse are
unchanged. The final delivery commit is the commit adding this document.

## Tooling

`test_data/round4_policy_session_measure.py` uses B's unchanged frozen generator
and complete typed/ordered oracle for independent CLI, session and pooled-session
layouts. The original B one-shot tool remains available. Its new counterpart
correlates every request status/timing/profile and public document, checks binary
and library identities, rotates orders and retains failures. See
`test_data/round4_policy_session_README.md` for configuration and commands.

Ten focused Python gate tests passed; targeted lint reported zero errors and
warnings. A real declared Old large two-hop timeout exposed an overly narrow
measurement gate: the query deadline expired in `fragment_transport.cc`, rather
than Gather. The tool now recognizes that precise source/cause only for declared
case/mode with exit1/timing1 and no current public document. The query remains
failed. Other transport, cancellation and worker-exit errors remain rejected.
The first failed gate and its raw output are retained externally, not counted as
the final campaign. No production error/deadline behavior was changed.

## Measured configuration and results

Release/Clang21, YB 2.31.0.0; immutable matching runtime snapshot
`ca6707c2d937`, integrated in the product base. The version string retains its
development base revision, so source/runtime manifests, not a relabeled build,
establish equivalence. This was WSL + one cloud host, one master and three
tablet servers (two cloud, one WSL), RF3, mixed leaders, four tablets per index.
Each host used two CPU affinities; one sequential coordinator and max two native
workers; page16/batch128 and worker8MiB/Gather64MiB accounting budgets.

The frozen B input has 32,314 triples, SHA256
`96df1b1181fac7f7a1c8e9e4d33abbd4d6edf73c2e6b6b0daa7086af1650819b`.
All 17 cases were exercised in pooled Old/New/Auto sessions. Across the final
51 pilot + 162 measurement requests, 212 complete results passed the
independent full typed-bag/projection/ordered-slice checks. There were
1 predeclared query-deadline failures and zero unexpected failures.
Failures expose no current result, remain failures and cannot supply a headline
successful-query latency. Later recovery requests are checked independently.

The formal comparison uses shared1024, unique1024 and twohop1024 once per group,
with 8,192/1,024/4,096 rows respectively. Nine mode/layout cells each have one
warmup and five complete formal groups. Group wall sums complete child-process
lifetimes: three independent processes versus one session process. Startup,
initialization, output and cleanup are included; Python validation and gaps
between calls are excluded. This is not server-only execution time.

| Mode | Independent seconds | Session seconds | Pool seconds | Pool reduction vs session |
|---|---:|---:|---:|---:|
| old | 36.569 | 27.110 | 33.656 | -24.1% |
| new | 25.348 | 15.123 | 14.371 | 5.0% |
| auto | 25.043 | 15.823 | 15.266 | 3.5% |

Negative reductions are regressions. Pool starts fall from 6+6 to 1+1 per
three-query group, with 12 OPEN and 10 runtime-reuse acknowledgments. This does
not guarantee lower wall time. Old regressed with pooling; New/Auto median
differences are small relative to overlapping ranges. Keep worker reuse
default-off; explicit sessions already remove repeated initialization. Do not
generalize earlier small-input gains to this larger cross-host input.

All 162 formal request positions, complete-group native starts, four-fragment
execution and zero coordinator fallback were checked. Auto retained Old for
independent starts and graph execution for shared/two-hop representative cases.
Resource observations cover the local query-process family and real cloud native
workers; they exclude daemons and are sampled, not RSS caps or leak proofs.

## Reproduction, limitations and rollback

Validate actual cross-host UTC skew over multiple sync periods before starting.
The operator used a volatile low-latency Linux time-source override for this
campaign and restored it after cleanup. No database clock protection was relaxed.
The Windows administrator correction script was not applied: read-only checks
showed its clock had already converged after the earlier service startup.

Operator artifacts include exact private config, source/input/runtime hashes,
all raw outputs, profiles, ranges/MAD, native PID/birth logs, resource samples,
independent verification and owned-process cleanup under
`round4-crosshost-20261104-150435-deadline`. Earlier clock/gate failures and the
separate cloud-only campaign remain retained. The default cloud runtime link was
not replaced. No cloud instance was released or stopped.

This bounded two-host test is not a three-physical-host failure-domain test,
general SPARQL certification, long-running concurrent service, scalability,
production HA or external SOTA comparison. B's earlier Debug test-use failure
was not repaired/retested here; no full C++ suite or all-build-types pass is
claimed. Do not combine the other machine's Debug/Clang17 times with these
Release/Clang21 results to compute a version speedup.

Rollback is an ordinary revert of this measurement-only delivery. No schema,
wire format or deployment migration is required. Publication must be independently
verified and must preserve shared-branch history.
