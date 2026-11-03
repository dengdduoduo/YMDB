# Round-three B handoff

Prepared 2026-11-03; work started 2026-11-02. No production Auto patch was made.
The independent oracle, measurement tools and bounded evidence are delivered;
universal or newly optimized performance is not claimed.

## Versions and publication

- BASE_SHA: `27c156736d3d4ff62e34bb141ffd1f6d5b500692`, GitHub `dengdduoduo/YMDB:ymdb-ckr`.
  It descends from checkpoint `22bd677a633d7da5d64e3af68444f06d57d85cc7`.
- Measured B code: `f2c10c10a63e9a5314d0f411f6c3b1e7ecf65619`; initial helpers: `b53c9e85f519aab38b9be6722dbb944620f160ee`.
- Matching VM source: `07321cc191e4f8052ec3059eab2dc6c4e504acc2`, branch `codex/round3-policy-vm`.
  All 3,609 managed MDB files match; source digest:
  `a28578d51b769f04049b1638916446ab256fc386218ae3277feb447dae711f1c`.
- Runtime: YB 2.25.1.0-b0, Debug/Clang17. Executable/linked-library identities
  match before/after measurement. The helper follow-up changed no compiled code.
- Final delivery HEAD is the documentation commit containing this report,
  recorded in the external `delivery-result.json` and Chinese execution log.
  This final documentation delta does not replace the measured runtime SHA.
- Target is `https://github.com/dengdduoduo/YMDB.git`, `refs/heads/ymdb-ckr`.
  Local branch `codex/round3-policy` tracks that ref. Publication requires the
  coordinated upload window from checklist section 3.2; no push is claimed here.
  The pre-delivery refresh found the remote still at BASE_SHA, with no new commits.

## B01-B06 status

| Step | Status | Evidence |
|---|---|---|
| B01 | Passed: verified base, build, original CLI | baseline manifests/build logs; small-pilot |
| B02 | Passed for frozen bounded cases | independent oracle tests; 50 full typed pilot results; one separate deadline failure |
| B03 | Completed cost/selection diagnosis | per-run profile, identity and analysis-v2.csv |
| B04 | Not implemented: insufficient causal evidence for a safe minimum policy change | policy-conclusion-pre-formal.json |
| B05 | Measurement completed; deadline failures retained; no new optimization success | 150 formal samples plus 30 warmups |
| B06 | Code/report ready; coordinated publication remains separate | local commits, delivery/cleanup records |

Changes are the B-specific C++ control test, two-line CMake registration, four
Python fixture/measurement/test files, the tool guide and this report.
`ChooseGraphExecution`, `GraphDecision`, CLI, worker/session/protocol, explicit
Old/New behavior, default Old and public budgets remain unchanged.

## Results and cost boundary

Each successful performance cell has five full-result formal samples. Times
include one-shot process startup, initialization, workers, publication and exit.
Ranges and MAD are in `performance-v2.csv`; no stage medians are added together.

| Query | Old s | New s | Auto s | Auto choice | Auto / Old |
|---|---:|---:|---:|---|---:|
| incoming1024 | 14.828 | 14.836 | 14.875 | graph | 1.003 |
| incoming4096 | 17.104 | 17.090 | 17.021 | graph | 0.995 |
| shared1024 | 14.468 | 14.703 | 14.431 | graph | 0.997 |
| shared4096 | 16.994 | 17.431 | 16.793 | graph | 0.988 |
| skew1024 | 15.709 | 15.084 | 14.972 | graph | 0.953 |
| skew4096 | 20.917 | 19.027 | 20.429 | graph | 0.977 |
| unique1024 | 16.718 | 17.196 | 17.206 | old | 1.029 |
| unique4096 | 24.807 | 24.304 | 24.452 | old | 0.986 |

These local medians are close overall (Auto/Old 0.953-1.029). Overlapping ranges
and initialization variation limit performance conclusions. Auto retains Old on
independent starts and graph execution on shared/incoming/skew/two-hop shapes.
Actual accepted graph runs used four graph fragments, at most two concurrent
local workers, with no coordinator capability fallback.

Deadline contracts are separate from the successful performance table:

| Query | Old complete / 5 | Old deadlines / 5 | New complete / 5 | Auto complete / 5 |
|---|---:|---:|---:|---:|
| twohop1024 | 5 | 0 | 5 | 5 |
| twohop4096 | 0 | 5 | 5 | 5 |

The twohop1024 complete formal sample medians were Old 31.629, New
15.772, Auto 16.356 seconds. An earlier Old warmup timed out;
both that failure and these completions remain visible. Twohop4096 Old has no
successful latency median and no speedup ratio. Each deadline failure exited 1
with zero public bytes; partial internal rows were not treated as a result.

Final campaign: 180 requests = 174 complete typed passes and 6 declared deadline
failures (5 formal, 1 warmup), with zero unexpected failures. Of 150 formal
samples, 145 have complete correct results; the 5 failures are all twohop4096 Old.

Auto planning medians were 106.7-189.3 ms, containing candidate
lowering, the bounded probe and any legacy replanning. Every Auto scan used one
explicit probe read of at most 16 rows, grouped by actual SPO owner, without
historical statistics. An isolated probe stopwatch is unavailable. Complete-cell
CLI initialization medians were 9.459-10.354 seconds. Large pilot
wall differences often came from initialization, not the selector. No safe
policy patch was justified by those observations, and the historical WAN unique
regression was not reproduced or repaired by this local experiment.

Read counters are explicit operations, not physical network RPCs. Maximum
observed simultaneous query-tree RSS was 343.4 MiB; shared pages can be counted
more than once and daemons are excluded. Worker 8-MiB/Gather 64-MiB accounting
budgets are not RSS caps. Whole-system peak RSS, isolated probe time and complete
worker buffer peaks remain missing.

## Inputs, tests and reproduction

Input has 32,314 triples; SHA256
`96df1b1181fac7f7a1c8e9e4d33abbd4d6edf73c2e6b6b0daa7086af1650819b`.
1024/4096 are two per-query input scales in one combined database; this is not an
initialization-scaling comparison between independently sized databases.
Typed bags, projection order and ordered OFFSET/LIMIT sequences are independently
enumerated/derived; COUNT and old-engine output are not the sole oracle.
Historical shared1024/unique1024/twohop512 inputs remain separate and unchanged.

Linux: five fixture/oracle tests and seven measurement-gate tests passed.
Six C++ cases passed: three B controls, complete-bag/mode selection,
direction/multiplicity/constraints, and worker/Gather budgets/recovery.
The unchanged combined `GraphAutoProbeKeepsSnapshotAndBoundsAdmissionAndControls`
case failed at line 7493 by accessing temporary `Result::status()` before `ok()`
in Debug. Its preceding snapshot/admission/partition assertions executed, but
the complete case stays failed. Original log/XML are retained; the separate B
controls use the correct Result contract. No full-suite or independent 2.31
Linux build is claimed. Windows skipped the two POSIX process tests; Linux ran
all seven without skips.

Use [the tool guide](test_data/round3_policy_README.md), Python 3 standard
libraries, and the retained compiler/build root. C++ tests must go through
`yb_build.sh`, one case per invocation, with `ulimit -n 65536` for MiniCluster.
For the exact measured configuration and frozen cases:

```bash
python3 src/yb/mdb/test_data/round3_policy_measure.py \
  --config "$RUN_ROOT/formal-v2-config.json" \
  --manifest "$RUN_ROOT/fixture-v2/dataset-manifest.json" \
  --output "$RUN_ROOT/fresh-results" --warmups 1 --repeats 5 \
  --cases shared1024 unique1024 twohop1024 incoming1024 skew1024 \
          shared4096 unique4096 twohop4096 incoming4096 skew4096
```

The configuration explicitly declares Old two-hop deadline contracts. Failed
records stay `cli_failed`; incorrect complete results and partial public output
stop the campaign. The outer process timeout is 120 seconds; the unchanged
streamed query read view has its existing 30-second deadline.

## Evidence, cleanup and rollback

Raw inputs, full outputs, profiles, RSS observations, failed pilot/warmup,
imports/builds, test XML and source/runtime identities are retained in
`round3-policy-evidence.tar.gz` (SHA256 `d87eb2cf6ecdc354007dedbc8a1030e7e20a22b591ac4467a0f76dbec65720f0`).
Key files are `formal-v2/runs.jsonl`, `formal-v2/identity.json`,
`formal-v2-freeze.json`, `analysis-v2.csv`, `deadline-contracts-v2.csv` and
`cleanup.json`. Machine-specific locations and exact commands are in the private
Chinese execution log, not in repository fixtures.

Owned master/tserver processes stopped normally. All six dedicated ports are
closed, with no remaining query/worker process. Run data, failed inputs, prior
branches/backups and the four pre-existing untracked VM files are preserved.
Windows' original unborn/staged checkout was not used for publication.

Rollback uses ordinary reverts of the B commits; no production default, storage
format or wire protocol changed. Remaining work is coordinated publication and
the next-round A+B/session/multi-host acceptance owned by the other line. This
single-VM, one-tserver RF1/SPO+OPS-four-tablet result is not final cross-host or
external-database acceptance.
