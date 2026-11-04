# Session-aware policy measurements

`round4_policy_session_measure.py` adapts the round-three independent fixture
and complete typed/ordered oracle to three layouts: independent CLI processes,
a sequential session, and a session with explicitly enabled bounded worker reuse.
The original round-three one-shot tool remains unchanged.

## Reproduction

Generate the round-three fixture into a new empty directory. Provide a private
JSON config with the original fields `cli`, `worker_binary`, `masters`,
`keyspace`, `BASE_SHA`, `source_digest` and `build_version`. Optional fields:

- `worker_targets` (default `local`), `worker_ssh_config`, and
  `worker_ssh_target` for an already configured remote worker.
- `worker_identity_binary` names the real ELF executable when `worker_binary`
  is an immutable launch wrapper. The executable and linked libraries are hashed.
- `command_prefix`, e.g. `["taskset", "-c", "0,1"]`.
- `spawn_log` is an operator-owned launch log shared by path on both hosts.
  Each native worker invocation appends a line. SSH reads use the existing config.
- `process_timeout_seconds` (default 360) bounds each child process.
- `deadline_failure_contracts` has the same explicit query/mode format as the
  original B tool; it does not transform a timeout into a successful query.

The operator owns the database, isolated namespace, immutable matching runtimes,
CPU/resources, leader placement and eventual cleanup. This tool does not start
or stop database services, change production flags/defaults, or import data.

```sh
python3 src/yb/mdb/test_data/round4_policy_session_measure_test.py -v
python3 src/yb/mdb/test_data/round4_policy_session_measure.py \
  --config /private/config.json --manifest /private/fixture/dataset-manifest.json \
  --output /private/new-pilot --layouts pool --repeats 1 --warmups 0
python3 src/yb/mdb/test_data/round4_policy_session_measure.py \
  --config /private/config.json --manifest /private/fixture/dataset-manifest.json \
  --output /private/new-formal --cases shared1024 unique1024 twohop1024 \
  --repeats 5 --warmups 1
```

Modes default to Old/New/Auto. Query and mode/layout orders rotate. Output
must not exist. All input/dataset/query/expected digests are checked using the
unchanged B oracle. Keep configuration, data and runtimes fixed during a run.

## Measurement and failure boundaries

A fixed query-group wall is the sum of its complete child-process lifetimes:
three full independent process times versus one full session time. Each includes
initialization, query execution, result publication and cleanup. Python JSON
parsing/oracle verification and inter-process orchestration are excluded equally;
this is not server-only execution or a claim of client application loop time.
Per-request CLI initialization/execution and profiles retain request positions,
so first and subsequent requests can be analyzed separately.

Each session request must have one matching timing and ordered integer status.
Each successful request must have its own profile and one independently validated
complete document. Missing/extra documents, mismatched projection/types/multiplicity,
wrong ordered pagination, process cleanup failure and missing pool statistics stop
the campaign. A declared deadline retains `cli_failed` and no current public result;
earlier successful documents and later recovery must still validate. Any failed
group suppresses the headline performance median for its complete mode/layout cell.
Five complete formal groups per cell are needed for `performance_acceptance`.

Raw process output, extracted documents, diagnostics, commands, pool counters,
request outcomes, groups and summaries are retained. Startup counters observe
native launches, not worker lease counts. RSS is a sampled local query-process
family high-water observation, may double-count shared pages, excludes remote
native workers and daemons, and is not a memory cap or long-term leak proof.
Accounting budgets remain worker8MiB/Gather64MiB; maximum two workers per query.

A final independent checker should verify topology/runtime identities, actual
worker placement, graph/Auto choice, absence of unintended coordinator fallback,
initialization/reuse counts, all full results, resources and owned-process cleanup
before accepting performance conclusions. The tool does not certify arbitrary
query semantics, general network fault tolerance, scalability, HA or SOTA parity.

Declared query deadlines may expire in Gather or its frame reader. The latter
requires the actual fragment_transport.cc cause as well as the same explicit
case/mode, exit status and no-public-document checks. Connection, cancellation
and worker-exit errors are not accepted as successful or declared measurements.
