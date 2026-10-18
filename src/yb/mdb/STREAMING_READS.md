# Experimental transactional streaming reads

The existing cache path remains the default. To exercise the new path against
an initialized MDB keyspace:

```sh
mdb_cli --mdb_masters=127.0.0.1:7100 --mdb_keyspace=mdb_test \
  --mdb_streaming_reads=true --mdb_scan_page_rows=256 \
  query 'SELECT ?s WHERE { ?s ?p ?o } LIMIT 10'
```

Transactions must remain enabled. Every query owns its variable context,
read transaction, session, deadline, and cursors. A historical timestamp is
fixed before execution and reused across indexes and pages. This does not
promise the latest committed value. Any read restart fails the query instead
of continuing at a new timestamp.

`TriplePlan` feeds `DocDBScan` directly from `DocDBScanCursor` in this mode.
There is no cache preloading or cache fallback, including for empty matches.
Runtime bindings are reapplied when an index join resets its inner scan.
Stored IDs must have a typed dictionary mapping; missing mappings are errors.
Unknown query terms produce an empty matching pattern without inserting IDs.

Each storage page has row and decoded-triple byte limits. These limits are
not a total query memory budget: existing joins, sorting, dictionary state,
and CLI result buffering can still consume additional memory. Newer clients
rotate the session arena between synchronous pages to avoid retaining every
page's request and response allocations.

The C callback interface can deliver rows before a later failure. A nonzero
return value invalidates the whole result; callers must discard partial rows.
The CLI does so. Callbacks must not reenter MDB APIs or wait for another thread
to call them: public adapter operations currently serialize access to shared
dictionary state. Same-thread reentry returns an error. Direct model APIs and
legacy parser callers do not provide concurrent-query safety.

Physical scan bounds are encoded YCQL hash partition keys, with inclusive
start and exclusive end. They are distinct from logical RDF ID ranges. The
range cursor is a storage primitive; remote workers and distributed SPARQL
execution are not enabled. Scatter still rejects unsupported fragment bounds
and unproven duplicate full-query execution.

Relevant regressions are in `mdb_pipeline_e2e_test` and
`mdb_distributed_integration_test`. The integration fixture uses three tablet
servers in one MiniCluster and four tablets per index, not three machines.
Build and test evidence, including failures and local/VM version differences,
is maintained in the project's Chinese task log.
