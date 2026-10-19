# Bounded batches and index joins

Streaming reads can use BatchIndexJoin when the inner physical node is a
DocDBScan with a bound hash key and a guaranteed assigned join variable.
Other joins retain their existing operator. OPTIONAL, MINUS, and NOT EXISTS
are not converted into inner joins. This is coordinator-side batched storage
access, not remote SPARQL fragment execution.

BindingBatch carries a fixed column layout, column-major packed IDs, a bound
bitmap, row count, optional source-row numbers, and explicit EOF. Its buffers
are constrained by bytes as well as rows. BatchOperator exposes Open,
NextBatch, and Close with Status/Result. The legacy adapter continues after
empty non-EOF batches and propagates page failures instead of returning EOF.

An outer batch is grouped by its complete runtime probe bounds. Repeated keys
share one probe; all outer-row combinations are restored when emitting each
inner match. Routing uses official operation partition-key encoding and the
table's partition list. The client still owns final tablet routing/retries.
One bounded wave of independent page operations is queued on the query-owned
session and flushed together. No session is called concurrently from multiple
threads. Every continuation retains its query snapshot and original bounds.

Pages and high-fanout results are streamed rather than fully materialized.
Empty probes are cached only after EOF, with a bounded per-operator cache.
Reset reopens runtime bindings without changing the query read view. Budgets,
cancellation, deadlines, and errors are checked while serving buffered results.

The per-join memory guard accounts for binding buffers, grouped row lists,
negative-cache entries, decoded pages, retained row sidecars, and a conservative
per-cursor allowance. Shared dictionaries, client arenas/pools, allocator
overhead, and other operators are outside this guard. This is not P5's global
query memory tracker or spill implementation. Oversized configurations fail;
an operator does not silently switch algorithms after publishing results.

CLI controls (effective only with --mdb_streaming_reads=true):
- --mdb_batch_index_join=false selects the scalar reference path.
- --mdb_join_input_rows / --mdb_join_output_rows cap binding batches.
- --mdb_join_batch_bytes / --mdb_join_memory_bytes bound operator buffers.
- --mdb_join_max_in_flight caps page operations queued per flush.
- --mdb_join_negative_cache_entries caps empty-probe cache entries.
- --mdb_query_profile=true writes a JSON profile to stderr.

The calling-thread C adapter profile is also available through
YBCMdbGetLastQueryProfile. read_operations counts submitted YCQL page
operations, and flush_rounds counts non-empty flushes. Neither is a wire-RPC
counter. response_payload_bytes counts returned row sidecars, excluding RPC
envelopes and retries. The integration regression independently measures
TabletServerService.Read handler counts on all three tablet servers to verify
real request coalescing. Wall-clock speedup is not inferred from these counts.

Regressions cover byte limits, layout/null/origin roundtrips, empty non-EOF
batches, sticky errors, legacy adapters, 3-by-2 bag semantics, tiny pages,
reset and historical snapshots, real four-tablet routing, high fanout, bounded
empty-probe caching, cancellation/deadline recovery, and a failed operation
within a multi-operation page read. Existing RDF and streaming regressions
remain part of validation.
