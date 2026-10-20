# Experimental fragment execution

P6 adds a physical BGP representation and independent query worker processes.
The public adapter selects this path after parsing, rewriting, constant rebinding,
and creation of the query's read context. Workers never parse SPARQL or clone the
whole query. The existing streaming and join defaults are unchanged.

## Supported shape and ownership

The initial whitelist is SELECT projection over triple-only BGPs, inner joins,
and sequences of these operators. Repeated variables and duplicate result rows
retain their semantics. Expressions, property paths, OPTIONAL, UNION, DISTINCT,
ORDER BY, slicing and aggregation stay on the existing coordinator path. This
capability fallback happens before remote execution; a running fragment failure
never falls back or retries already emitted output.
Worker inner joins currently use bounded index probes. Forced hash/auto policies
remain on the P5 coordinator path rather than silently changing the requested
algorithm. Shipping batch/hash join operators is a later worker extension.

The first triple is the anchor. Its SPO hash ranges must cover the entire table
without gaps or overlaps. Each anchor triple belongs to exactly one task.
Inner probes may choose SPO, OSP or POS and can read any tablet; they do not
inherit the anchor range. Physical properties identify the anchor table,
partition function and variable, bound/output variables, and lack of global
ordering. Unknown cardinality/byte estimates remain unknown. Tablet counts or
equal numeric IDs are never used as proof of colocated joins.

## Transport and identity

`mdb_fragment_worker` consumes `fragment.proto` messages over stdin/stdout.
Every message has a bounded network-order length prefix, protocol version,
query/fragment/attempt/stream IDs and strict batch sequence. OPEN carries the
physical triple graph, projection schema, table generation IDs, persistent
`rdf_idmap` ID namespace, snapshot and resource limits. No C++ object layout,
pointer, parser term registry or process-private packed ObjectId is transmitted.
Only the coordinator decodes result IDs into RDF values.

Remote workers use `/usr/bin/ssh` with strict host-key verification, batch user
authentication, one connection attempt and bounded connection/liveness timeouts.
No new listening port or unauthenticated operator-execution service is added.
An optional coordinator-local SSH config selects existing keys, users and ports.
An optional worker-local gflag file configures the existing YB client's TLS
settings. Keep keys/certificates outside source and evidence archives.

`--mdb_worker_targets=host-a,host-b,host-c`
and `--mdb_worker_binary=/absolute/path/mdb_fragment_worker` enable this path
with `--mdb_streaming_reads=true`. Other controls are
`--mdb_worker_ssh_config`, `--mdb_worker_flagfile`,
`--mdb_worker_max_processes`, `--mdb_worker_batch_rows`,
`--mdb_worker_page_rows`, and `--mdb_worker_memory_bytes`.
The special target `local` starts isolated local processes for single-host
deployments and tests; it is not evidence of multiple physical nodes.

## Snapshot, flow control and failure

The coordinator exports the official YB child transaction data from its pinned
read transaction. Each worker creates its own client, transaction manager,
child transaction and session. All pages and inner probes use that child.
EOF includes FinishChild results, which the coordinator applies to the parent.
Read restart requirements fail the whole query. Table generation changes also
fail explicitly. Worker completion never commits or aborts the parent query.

Gather grants one outstanding batch credit per active stream and forwards a
bounded frame to the consumer. Workers keep only depth-first probe state,
bounded storage pages and the current output batch; they do not materialize
the result. A work quantum permits empty nonterminal batches. Each transport
frame is at most 1 MiB. Dimensions and a conservative buffered-state reservation
are validated before storage access. These limits exclude the YB client's
shared RPC infrastructure, table metadata and server-side allocations; they
are not a complete process-RSS or query-wide memory guarantee. There is no spill.

Cancellation, deadline, malformed/oversize/truncated frames, wrong stream IDs,
failed storage reads, authentication failure and worker disconnect terminate
the query. Local child processes are killed and reaped on scope exit. Closing
SSH ends the remote channel; a remote operation already inside a storage call
is additionally bounded by its read deadline and client operation timeout.
Consumers must discard rows from a failed query, as the CLI already does.
In worker mode the CLI stages complete output in an automatically removed
temporary file instead of a result-sized vector. Its default 256 MiB staging
limit is configurable with `--mdb_worker_output_spool_bytes`; write/space errors
or limit exhaustion fail the query without publishing staged rows. This is
bounded output staging, not join spill. Successful replay uses a fixed buffer.

## Validation scope

The fragment unit target tests ownership, framing, protocol rejection and
cancellation. The integration target uses three tablet servers, four tablets
per index and independent worker processes. It compares bag results with an
enumerable reference, tests shared snapshots under committed writes, slow
consumers, cancellation/deadline, insufficient budgets, worker death and recovery.
An SSH transport test uses a temporary loopback sshd and disposable keys without
changing system authentication files; it also rejects an untrusted host key.
That test requires OpenSSH client/server and key generation utilities.

Running these tests on one VM establishes process/transport correctness, not
cross-machine scaling. Separate physical-host acceptance and the newer native
YugabyteDB baseline build must be recorded independently. P7 global remote
operators and P8 exchanges/spill remain separate work.
