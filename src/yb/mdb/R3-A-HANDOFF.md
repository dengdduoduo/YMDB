# R3 A: sequential sessions and bounded worker reuse

Development base: 27c156736d3d4ff62e34bb141ffd1f6d5b500692 (ymdb-ckr).
A02 checkpoint: b2ae1c0023666b3634735fffeb6a2ab20f78778b.
The delivery commit is the commit adding this document: use
`git log -1 --format=%H -- src/yb/mdb/R3-A-HANDOFF.md`.
Remote synchronization must be verified separately; this file does not imply a push.

## Delivered behavior

- A01: version/baseline captured; legacy modes and Auto decision contract preserved.
- A02: explicit JSON `session file.rq ...` (1..1024 requests, 1MiB/request),
  sequential execution with successful initialization reused. Each request has a
  fresh read view and dictionary refresh. Failed requests contribute nonzero
  session status; subsequent requests can recover. Query output is staged.
- A03: opt-in `--mdb_worker_reuse=true`, session only, targets required, capacity
  1..16. A session owns the pool; each worker has one active lease. Capability
  negotiation, lease IDs/sequence checks, EOF/RELEASE/SHUTDOWN handshakes and
  per-request child transactions isolate state. Clock/client/table/manager
  infrastructure is reused. Idle processes are lazily recycled at 20 seconds or
  256 leases; workers stop after 30 seconds idle. Failed leases are discarded,
  without replay. A busy matching target is awaited instead of evicting another
  target's idle process at capacity.
- A04: result, snapshot/new-term, failure, cancel/deadline, capability, budget,
  identity and bounded-resource tests are provided. Tests include real SSH
  active cancellation/deadline recovery against an explicit external fixture.
- A05: same-version three-layout measurements and independent complete-result
  verification are archived. A06: this batch contains the A code and handoff;
  shared-branch publication and exact final SHA are recorded outside this file.

Example (all runtime paths supplied by the operator):

```sh
mdb_cli --mdb_result_format=json --mdb_graph_mode=new \
  --mdb_worker_targets=local,worker-host --mdb_worker_binary=/opt/ymdb/run/worker \
  --mdb_worker_max_processes=2 --mdb_worker_reuse=true session first.rq second.rq
```

Each successful SELECT/ASK emits one JSON line; updates may emit no document.
Use stderr `MDB_SESSION_REQUEST` index/status to correlate requests. On errors,
prior successful documents remain; the current failed query publishes no partial
rows. Output-channel failure terminates the session. `MDB_WORKER_POOL` diagnostic
is printed when query profiling is enabled.

## Validation and observed benefit

All listed executed cases were run individually through `yb_build.sh`. The A02
initialization/new terms and input/spool bounds, v3 release/old-worker rejection,
full Old/New/Auto result bags, failure recovery, idle/256-lease recycling,
real six-table namespace switch, 64-query FD/RSS bounds, 16->51->16 historical
snapshot, and language/datatype refresh are covered by focused regressions.
Budget regressions use full-result controls and exact error causes, with
success/failure/success for both DISTINCT retained state and output staging.
Normal defaults and late legacy-worker failure were rechecked before delivery.

Two-host RF3 validation uses three tablet servers (two on one host, one on the
other), mixed leaders, one sequential coordinator and at most two workers.
Old/New/Auto pooled sessions returned complete expected bags. Killing only the
owned worker SSH channel caused a failed request without replay and allowed the
next request to recover. The dedicated opt-in test also cancels/expires a query
after two distinct native workers acknowledged OPEN and before all rows arrive.

Two-host same-version performance (3720 triples; each group runs shared-key,
unique-key and two-hop queries; 1 warmup + 5 formal groups/layout):

| Layout | Three-query median wall | Formal range | Starts across both hosts |
|---|---:|---:|---:|
| Independent CLI | 12.952s | 12.485..15.049s | 12 |
| Session, pool off | 9.248s | 8.943..10.668s | 12 |
| Session, pool on | 7.637s | 7.138..8.496s | 2 |

Pool wall was 17.4% below session-only and 41.0% below independent CLI. All 54
performance SELECTs had independently verified complete typed bags/column order.
The earlier small local fixed-overhead diagnostic found 62.4% below session-only;
that result is separate and is not extrapolated to the two-host workloads.

## Reproduction and evidence

Build using the existing compiler/type through `yb_build.sh`; relevant targets
are `mdb_cli`, `mdb_fragment_worker`, `mdb_fragment_integration_test`.
For example, use the project's normal build options and one exact test per call:

```sh
./yb_build.sh release --cxx-test mdb_fragment_integration_test \
  --gtest_filter MdbDictionaryRecoveryTest.PoolBudgetsPublishNoPartialRowsAndRecover
```

The opt-in `MdbPoolSshTest.ActiveCancelDeadlineAndRecovery` requires
`MDB_POOL_CROSS_CONFIG`: a JSON file with masters, keyspace, target, binary,
ssh_config, predicate, edge, subject_prefix, object_prefix, subjects and objects.
It expects an isolated, already populated four-tablet/six-index fixture and
shared-key bag oracle; it does not create or stop infrastructure itself.
The lifecycle measurement diagnostic separately requires
`MDB_SESSION_BENCH_OUTPUT`, an existing persistent output directory.
Do not run diagnostics on an unrelated production namespace.

Detailed raw artifacts are retained by the operator under run IDs
round3-a01-20261102, round3-a03-boundaries-20261103,
round3-a04-isolation-20261103, round3-a04-resources-20261103,
round3-crosshost-fixed-20261103 and round3-closeout-20261103.
The measured product snapshot is ca6707c2d937; later closeout additions are tests
and documentation only. Its immutable cloud test package is named
ymdb-runtime-r3pool-20261103-141459-ca6707c2d937 (180 files verified).
The versioned package manifest maps product hashes to the dirty source snapshot;
do not claim a new binary merely because Git HEAD changed after testing.

## Upgrade/Rollback safety

Both CLI reuse and worker v3 capability default off. Existing v1/v2 behavior is
retained; an old worker explicitly rejects v3, and no silent fallback/replay is
performed. Deploy a complete matching CLI/worker/proto-library runtime. Keep
versioned packages immutable: pool keys include launch paths and storage IDs,
not binary/flagfile contents. After a deployment/TLS/configuration change, close
the old session. Roll back by disabling reuse, restarting sessions and selecting
the complete matching prior package. The stable cloud runtime link was not changed.

## Scope and limitations

This is bounded sequential reuse, not a concurrent service, distributed scaling
certification, long-term leak proof or SOTA comparison. RSS samples are not
allocator budgets or summed simultaneous peaks. Cross-host fault evidence is
limited to the recorded scenarios, not all network-partition combinations.
No graph storage layout, Auto decision contract, HashJoin/Leapfrog default,
arbitrary paths, general shuffle or production HA was introduced.
All owned test storage/worker/tunnel processes were stopped after each run;
historical datasets and failed-run artifacts were preserved.

Before publishing to shared ymdb-ckr, save local commits, fetch its actual head,
review/integrate changes with normal history-preserving operations and verify
affected behavior. Do not force push, rebase or overwrite a collaborator's work.
The A+B combined acceptance and external database baseline remain a later round.
