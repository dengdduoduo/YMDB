# P8 first exchange/spill increment

The public SPARQL adapter has an opt-in disk-backed exchange for two-triple
equi inner joins. Default execution remains P6/P7. This milestone has local
partition consumers on the coordinator, not peer-to-peer worker shuffle or
multi-host performance acceptance. Legacy hash/auto policies keep their
existing pre-launch fallback.

## Flow and ownership

The existing FragmentPlan validates the complete SELECT envelope, then requires
exactly two triples with shared bound variables. All shared variables, sorted by
ID, form the composite key. Unsupported shapes fall back before remote launch;
invalid resource configuration fails. Each side is independently scanned over
disjoint SPO subject ranges by existing workers, using the same parent read view
and child transaction export/apply. Transport keeps one pull credit per worker;
the synchronous disk sink supplies backpressure. Both scans finish before any
join output. There is no runtime retry or fallback.

Hash mode routes both inputs by FNV1a64-LE-v1 over persistent storage-ID key
tuples, modulo partition count. Broadcast mode round-robins left occurrences and
copies every right occurrence to every partition. Each result pair has one
owner, preserving complete duplicate bags. Packed RDF ObjectIds and lexical
strings are not exchanged or used as disk keys.

## Spill and skew

Every query creates its own 0700 mkdtemp directory and 0600 close-on-exec files.
The intermediate format contains MDBX0001, width, little endian uint64 IDs and
per-record FNV checksums. Readers check headers and exact length; consumed rows
check checksums and nonzero IDs. I/O errors and truncation are failures, not EOF.
These are actual join intermediates, separate from CLI final-output staging.

The existing P5 fixed-capacity HashJoin consumes disk binding sources. Oversized
right partitions are processed as disjoint ranges fitting its build capacity;
each range rescans its left partition. Even one all-equal hot key therefore has
bounded retained state and complete Cartesian bag multiplicity, without endless
recursive repartitioning. This can perform many rescans. The skew counter detects
build partitions exceeding a multiple of average load, not exact heavy keys.
Broadcast also spreads hot left occurrences over its local partitions.

The reservation covers metadata/I/O slack and HashJoin arrays, bindings and
batches; it is not total RSS. Worker frame budgets, dictionaries, YB client/RPC
arenas and P7 global admission remain separate. ORDER BY/DISTINCT/aggregation
still materialize and can fail their global budget after an exchange join.
External sort and external DISTINCT/aggregation are not implemented here.

Disk quota counts headers, records and every Broadcast copy. Quota exhaustion
fails without enlarging limits. Completion, cancellation, deadline, source or
consumer error and exceptions clean private files; cleanup failures are counted
and surfaced. No fsync, crash recovery or orphan scavenger is provided: SIGKILL
or power loss can leave private directories. The public callback can have seen
rows before a later error; the existing CLI discards its staged failed output.

## Configuration and statistics

With existing streaming-read worker targets and binary:

* --mdb_exchange=none|hash|broadcast (default none)
* --mdb_exchange_partitions=4 (2..64)
* --mdb_exchange_memory_bytes=8388608 (64 KiB..256 MiB; metadata must fit)
* --mdb_exchange_spill_bytes=1073741824 (up to 1 TiB)
* --mdb_exchange_spill_directory=/tmp (existing absolute directory)
* --mdb_exchange_skew_factor=2 (1..16)

Exchange without worker targets is an error. Profiles record input/output rows,
Broadcast copies, file/live-file counts, cumulative reserved disk bytes, rows
read back, peak consumer reservation, largest partition, join/chunk passes,
skew partitions and cleanup failures. With exchange enabled remote_rows counts
both scans, while global_input_rows counts joined rows passed to P7 modifiers.
Four-tablet input scans normally launch eight fragments for a two-side join.

## Verification boundary and next milestones

Tests exercise composite bags, Hash/Broadcast ownership, forced-small-memory
hot keys, disk quota, sticky input errors, consumer errors, cancellation and
deadlines, corruption/truncation and cleanup. Integration tests use the actual
adapter, existing isolated three-tserver/four-tablet cluster, and independent
workers. Snapshot coverage mutates storage between scans and checks one read
view. CLI tests cover flags and failed-query output discard. Passing results
must come from the stage evidence archive, not this document alone.

Remaining: worker-to-worker receivers, bounded network shuffle scheduling,
parallel consumers, automatic mode choice, external P7 modifiers, wider join
graphs, adaptive skew placement and orphan cleanup. Real multi-host acceptance
and an independent 2.31 Linux build remain separate pending work.
