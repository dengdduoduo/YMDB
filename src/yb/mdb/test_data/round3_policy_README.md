# Round-three policy fixtures and one-shot measurement

These tools belong to line B. They require only Python 3 standard libraries and
the existing `mdb_cli`/`mdb_fragment_worker`. They do not change CLI, profile,
worker lifecycle, protocol, session interfaces, or default execution mode.

## Independent inputs and oracle

Run from the repository root:

```bash
python3 src/yb/mdb/test_data/round3_policy_fixture.py generate --output /tmp/r3-input
python3 src/yb/mdb/test_data/round3_policy_fixture_test.py
```

The destination must be empty. No historical input is overwritten. Keep the
round-two `graph_native_benchmark.py` and its shared1024, unique1024, twohop512
fixtures separately for association; they are not modified by these tools.

The combined dataset contains 32,314 distinct triples. Every shape has 1024 and
4096 seed rows. Distinct predicate namespaces isolate shapes within one dataset:

| Shape | Seed-to-key rule | Expansion | Complete output rows |
|---|---|---|---|
| shared | all seeds use key 0 | eight outgoing neighbors | 8N |
| unique | seed i uses key i | one outgoing neighbor i | N |
| twohop | seed i uses key i modulo 16 | four witnesses, leaf j modulo 7 | 4N |
| incoming | all seeds use key 0 | eight incoming neighbors | 8N |
| skew | first 3N/4 use key 0; remaining seeds have independent keys | hot key has eight neighbors; other keys have one | 25N/4 |

Large expected bags are derived directly from seed indices. A separate indexed
BGP interpreter in the test reads the emitted triples and checks those formulas.
Small fixtures enumerate typed results, two distinct witnesses for identical
projected rows, repeated VALUES, a bound neighbor, empty results, DISTINCT,
reverse expansion, and ordered pagination. No old-engine result supplies the
oracle. Literal lexical value, datatype and language are compared exactly.
Generated IRI indices have six digits, making the pagination oracle independent
of numeric-vs-lexical sorting ambiguity. The ordered case validates both the
complete bag and the actual sequence after OFFSET/LIMIT.

Each manifest binds dataset, query, expected gzip and generator digests. Every
case declares its output rows, a 120-second outer process timeout, 8-MiB worker buffer
budget and 64-MiB Gather budget before execution. The largest expected output is
32,768 rows. These are bounded test inputs and accounting budgets, not RSS caps.
Two input sizes in one dataset do not measure scaling of initialization against
two independently sized databases.

## Measuring the current CLI

Create a private JSON configuration outside the repository:

```json
{
  "cli": "/absolute/build/bin/mdb_cli",
  "worker_binary": "/absolute/build/bin/mdb_fragment_worker",
  "masters": "127.0.0.1:17100",
  "keyspace": "isolated_round3_policy",
  "BASE_SHA": "verified-public-base-sha",
  "source_digest": "verified-managed-source-digest",
  "build_version": "actual-source-and-compiler-version"
}
```

Import into a dedicated database, then run:

```bash
/absolute/build/bin/mdb_cli --mdb_masters=127.0.0.1:17100 \
  --mdb_keyspace=isolated_round3_policy import /tmp/r3-input/dataset.nt
python3 src/yb/mdb/test_data/round3_policy_measure.py \
  --config /private/r3-config.json --manifest /tmp/r3-input/dataset-manifest.json \
  --output /private/r3-results --warmups 1 --repeats 5
```

The output directory must not exist. For a correctness pilot, use zero warmups
and one repeat; that pilot is not five-sample performance acceptance. Select
specific frozen cases with `--cases shared1024 unique1024`. Formal runs rotate
cases and mode order, retain warmups, validate every complete typed output, and
stop at the first failed correctness/timeout/profile gate. Failed artifacts are
retained in the same runs.jsonl as successes. Do not mix a budget-failure
contract campaign into the successful performance table.

Every record contains source/config/input/query identities, argument list,
result and diagnostic paths, repeat/warmup, status, wall time, full profile and
CLI timing, oracle verdict and observed RSS. The identity file also hashes
executables and linked libraries before/after the campaign. Record a rebuilt
runtime as a different campaign. The runner uses local workers, two concurrent
processes, batch 128, storage page 16, and unchanged public buffer defaults.

Wall timing spans process launch through exit, including initialization,
workers, full output and cleanup. Oracle verification is outside this boundary.
`adapter_plan_us` includes graph candidate planning and bounded Auto sampling;
it is not an isolated decision stopwatch. Storage operation counts are explicit
reads, not physical network RPC counts. Unavailable metrics are listed as
missing. Phase medians must never be added to fabricate the wall median.

Linux `/proc` is sampled every 50 ms for the query process tree. Per-process
observed peaks and simultaneous observed tree RSS are different fields; neither
includes database-daemon RSS nor guarantees capture of the actual peak. The
summary reports medians, min/max and median absolute deviation. Buffer
accounting and RSS remain distinct. The tool never changes server state or
stops a shared service.

## Declared deadline contracts

The existing streamed query read view retains its 30-second internal deadline.

Keep exhausted query budgets separate from successful performance cells. An
observed deadline may be frozen explicitly in the private configuration:

```json
{
  "deadline_failure_contracts": [{"query_id": "twohop4096", "mode": "old"}],
  "case_classes": {"twohop4096": "deadline_contract"}
}
```

These fields supplement the required configuration above. A contract continues
only for the declared query/mode, CLI exit and timing status both equal to 1, the exact
Gather deadline diagnostics, and an empty public output. The record remains
`cli_failed`, and its complete-result oracle remains false. All other errors,
partial public output, and incorrect successful results stop the campaign.
`complete.json` distinguishes a completed campaign from all queries passing.
Failed cells have no success-only headline median. Contract-class cells never
receive performance acceptance, including their successful modes.

Run `python3 src/yb/mdb/test_data/round3_policy_measure_test.py` to check the
failure gates, preservation of failed statuses, and stop-on-wrong-result behavior.
