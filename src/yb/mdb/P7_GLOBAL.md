# P7 global fragment finalization

P7 extends the existing P6 SELECT fragment path. No schema, index layout,
worker wire format or snapshot mechanism changes are required.

## Execution and capability boundary

The coordinator validates the entire root SELECT envelope before launching
any worker. Workers execute only the mutually exclusive anchor scans and BGP
joins from P6. For an accepted global query, every bound BGP variable is returned
as a persistent storage ID, including non-projected sort/group keys. Gather
preserves the complete input bag, maps IDs to typed ObjectIds, and applies every
child transaction result. Only after all fragments complete does it replace the
BGP input with an internal Values operator. The original global modifier tree is
constructed/executed once, without reparsing or opening another snapshot.

Supported envelopes:

* SELECT DISTINCT, variable ORDER BY (multiple keys, ASC/DESC), OFFSET/LIMIT,
  including DISTINCT followed by slicing and LIMIT 0.
* COUNT(*), COUNT(?variable), COUNT(DISTINCT ?variable), COUNT(DISTINCT *),
  with optional simple-variable GROUP BY. Empty ungrouped input counts as zero;
  empty grouped input has no groups.
* Grouped COUNT results can be ordered by aggregate aliases and grouping
  variables (including hidden group keys), then DISTINCT and OFFSET/LIMIT.
  Group-only SELECT ordering and ordered ungrouped COUNT are supported too.
  The constructor defers grouped ordering until aggregate bindings exist.
* Bounded HAVING predicates: COUNT variants, int64/boolean
  constants, six comparisons and AND/OR/NOT. Relational operands are COUNTs or
  int64 constants; group-variable operands are supported in equality/inequality.
  HAVING-only aggregates are registered as private slots. Conditions run after
  aggregation and before ordering, DISTINCT and slicing. SELECT expression
  aliases are not visible in HAVING, as required by SPARQL's scope rules.
  Integer constants include typed dictionary identities with at most 21 lexical
  bytes; they are not assumed to use the legacy inline representation.
* Global term identity uses the existing typed RDF mapping, not printed text or
  raw numeric storage-ID ordering. ORDER BY uses compare_for_order.

Fallback is decided before remote work: OPTIONAL, UNION, subqueries, paths,
FILTER/BIND envelopes, computed ORDER BY/GROUP BY/SELECT expressions,
other aggregates and constant-only BGPs retain the existing coordinator path.
Grouped ordering keys must be group variables or projected COUNT aliases;
computed ordering and non-grouped keys remain outside this capability.
This capability fallback does not certify unimplemented legacy operator
semantics. General HAVING expressions, projection expressions and numeric/distinct aggregates
remain incomplete. P7 does not silently advertise these as distributed
capabilities. Hash/auto join policies
also retain the P5 coordinator implementation.

Global Gather budget exhaustion, dictionary failure, worker failure, deadline or
cancellation fails the query; there is no runtime retry or coordinator fallback.

## Resource and output contract

WorkerOptions::coordinator_memory_bytes (CLI --mdb_global_memory_bytes) defaults
to 64 MiB. Each gathered row reserves
1024 + 128 * final_variable_count + 16 * count_distinct_star_sets * final_variable_count
+ 16 * order_key_count + 64 * having_node_count bytes,
covering input vector growth and conservative per-row slack for retained binding
arrays, sort keys/indices, DISTINCT trees and COUNT DISTINCT sets. This is a bounded
input admission reservation. Each COUNT(DISTINCT *) is charged for its own full
binding tuple set, including when several appear in one projection. The budget
also charges every explicit ordering key, including repeated variables;
at most 128 keys are admitted. The group-input sort and final-output sort can
coexist, with no more grouped output rows than input rows (except the single
empty-input ungrouped aggregate row). This is
not measured process RSS or a universal allocator
limit. Shared dictionaries, YB client infrastructure and output decoding are
excluded. Unsupported unbounded expression/concatenation aggregates are not
admitted into this path. Exceeding the reservation fails before global output.
final_variable_count includes one future private slot per HAVING COUNT;
count_distinct_star_sets includes both SELECT and HAVING sets. HAVING is limited
to 128 nodes, depth 16, 16 aggregates and 128 final variables. These bounds are
checked before remote work. The small bounded expression tree and empty-input
aggregate state are not a separately metered allocator budget. Worker frames
still carry only the original BGP variables and persistent storage IDs.

The COUNT predicate boundary excludes arithmetic, functions, computed COUNT
arguments, nested/other aggregates, general literals, SELECT-alias references
and bare group variables as effective-boolean conditions. Unsupported forms
retain the coordinator path before launch. Invalid comparisons are expression
errors: they reject the group unless SPARQL boolean rules resolve the error
(for example error OR true). Null effective-boolean conversion now preserves
the error instead of turning NOT(error) into true.

Statistics include global_input_rows, global_reserved_bytes and
global_finalize_operators. remote_rows describes pre-finalization input, not the
number of final output rows. P7 drains every accepted fragment, including LIMIT
queries, before applying global operations. There is no early Top-K pushdown,
external sorting or spill; large inputs require P8 or a larger explicit budget.
Existing CLI output staging still discards staged output on failure.

The public adapter's existing cell callback has no row-boundary/unbound event.
P7 retains that API; it does not claim to repair its representation of rows with
unbound projected cells.

## Validation boundary

Functional tests run through YBCMdbExecuteSparql against the existing isolated
three-tserver, four-tablet MiniCluster, with independent local worker processes.
Tests compare ordered sequences or full bag cardinality and explicit expected
values, including typed terms, global duplicates, non-projected ordering keys,
empty results, COUNT DISTINCT, groups, failure/budget recovery and pre-launch
fallback. This environment is a single VM and cannot establish real multi-host
scaling. P6 multi-host acceptance and the independent 2.31 Linux baseline build
remain separate pending requirements. A test/source archive records actual
build/test results; this document alone is not evidence of a passing run.

The ordered-group extension adds explicit complete-row expectations for unequal
counts spanning four tablets, mixed directions and tie-breaks, hidden group
keys, grouped COUNT DISTINCT variants, DISTINCT before slicing, empty inputs,
LIMIT 0 and out-of-range offsets. Budget accounting, failure without fallback,
callback cancellation/deadline and subsequent recovery have dedicated coverage.
HAVING coverage additionally checks hidden aggregates, SELECT-alias scope,
empty ungrouped COUNT, all comparison operators, multiple conditions, boolean
errors, private-slot/tuple-set reservations and pre-launch complexity limits.
Semantics follow SPARQL 1.1 sections 18.2.4 and 18.2.5:
https://www.w3.org/TR/sparql11-query/#convertSolMod
