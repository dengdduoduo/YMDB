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
  constants, six comparisons and AND/OR/NOT. Relational operands are aggregates or
  int64 constants; group-variable operands are supported in equality/inequality.
  HAVING-only aggregates are registered as private slots. Conditions run after
  aggregation and before ordering, DISTINCT and slicing. SELECT expression
  aliases are not visible in HAVING, as required by SPARQL's scope rules.
  Integer constants include typed dictionary identities with at most 21 lexical
  bytes; they are not assumed to use the legacy inline representation.
* Global term identity uses the existing typed RDF mapping, not printed text or
  raw numeric storage-ID ordering. ORDER BY uses compare_for_order.
* SUM/AVG/MIN/MAX with variable arguments, including DISTINCT, finalize over
  the complete gathered input. SUM/AVG support integer/decimal/float/double
  promotion; DISTINCT uses typed RDF identities across all fragments. MIN/MAX
  use the same value/term ordering as ORDER BY, ignoring unbound inputs.
  HAVING accepts these aggregates and SELECT extensions accept numeric SUM/AVG.
  MIN/MAX have unknown result types and are not advertised as arithmetic inputs.
  Empty ungrouped SUM/AVG return integer zero, MIN/MAX remain unbound. SUM of
  expression errors or non-numeric terms is unbound; AVG with zero bound values
  returns zero, otherwise such errors make it unbound.
  Integer SUM totals are exact up to signed int64 result range, with overflow
  represented as an unbound result; prefixes do not prematurely overflow.
  Decimal inputs/results are bounded to 18 significant digits, exponent
  -100..100, with result rounding to that precision and at most 256 lexical
  input bytes. Values outside these decimal bounds yield an expression error.
  Floating arithmetic follows the existing IEEE float/double model, including
  canonical INF/-INF/NaN output. This does not claim arbitrary precision.
  Ordered comparisons with NaN are false; ORDER BY/MIN/MAX place NaN before
  other numeric terms deterministically. Integer/decimal comparison uses exact
  normalized digits instead of converting their values to double.
* Post-aggregate SELECT expressions support COUNT variants, signed int64
  constants, addition/subtraction and preceding numeric SELECT aliases. They
  run after HAVING and before ORDER BY, DISTINCT and slicing. Forward aliases
  and SELECT aliases in HAVING are outside scope. Integer overflow and unbound
  operands produce an unbound expression result. Arithmetic is bounded int64,
  not arbitrary-precision xsd:integer arithmetic.

Fallback is decided before remote work: OPTIONAL, UNION, subqueries, paths,
FILTER/BIND envelopes, computed ORDER BY/GROUP BY expressions,
other aggregates, computed aggregate arguments and constant-only BGPs retain
the existing coordinator path.
Grouped ordering keys must be group variables or projected expression aliases;
computed ordering and non-grouped keys remain outside this capability.
This capability fallback does not certify unimplemented legacy operator
semantics. General HAVING expressions and other projection expressions
remain incomplete. P7 does not silently advertise these as distributed
capabilities. Hash/auto join policies
also retain the P5 coordinator implementation.

Global Gather budget exhaustion, dictionary failure, worker failure, deadline or
cancellation fails the query; there is no runtime retry or coordinator fallback.

## Resource and output contract

WorkerOptions::coordinator_memory_bytes (CLI --mdb_global_memory_bytes) defaults
to 64 MiB. Each gathered row reserves
1024 + 128 * final_variable_count + 16 * count_distinct_star_sets * final_variable_count
+ 16 * order_key_count + 64 * having_node_count + 512 * computed_select_node_count
+ 512 * having_value_aggregate_count bytes,
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
final_variable_count includes future private slots for HAVING and nested SELECT aggregates;
count_distinct_star_sets includes both SELECT and HAVING sets. HAVING is limited
to 128 nodes, depth 16, 16 aggregates and 128 final variables. These bounds are
checked before remote work. The small bounded expression tree and empty-input
aggregate state are not a separately metered allocator budget. Worker frames
still carry only the original BGP variables and persistent storage IDs.
Computed SELECT expressions have a combined limit of 128 charged nodes and
depth 16; HAVING and SELECT together admit at most 16 private aggregate slots.
Direct projected COUNTs retain their existing admission charge. Each computed
node reserves space for intermediate integer dictionary identities and row
state. Unknown-type group variables in arithmetic, functions, multiplication,
division and other aggregates retain the coordinator path before launch.
Each SELECT value aggregate charges one computed node (512 bytes per input
row); each HAVING value aggregate adds another 512 bytes. This covers bounded
numeric state, result identities and its own scalar DISTINCT tree. MIN/MAX
need no DISTINCT tree because duplicate elimination cannot change an extremum.

The HAVING predicate boundary excludes arithmetic, functions, computed aggregate
arguments, nested/other aggregates, general literals, SELECT-alias references
and bare group variables as effective-boolean conditions. Unsupported forms
retain the coordinator path before launch. Invalid comparisons are expression
errors: they reject the group unless SPARQL boolean rules resolve the error
(for example error OR true). Null effective-boolean conversion now preserves
the error instead of turning NOT(error) into true.

Statistics include global_input_rows, global_reserved_bytes and
global_finalize_operators. remote_rows describes pre-finalization input, not the
number of final output rows. P7 drains every accepted fragment, including LIMIT
queries, before applying global operations. There is no early Top-K pushdown
or external global finalization. The opt-in P8 two-input join exchange has its
own spill path (P8_EXCHANGE.md); P7 global state retains its admission limit.
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
SELECT coverage checks complete grouped and ungrouped results, alias chains,
hidden COUNT variants, arithmetic error propagation, ordering and slicing,
empty inputs, exact reservation boundaries and recovery after worker/output
failure. Unit tests exercise both signed int64 overflow boundaries.
Value-aggregate coverage checks all four aggregates, DISTINCT, value ordering,
integer cancellation/overflow, decimal/float/double promotion, empty and error
inputs, exact budget admission and cancellation/failure recovery. These tests
do not establish multi-host performance or arbitrary-precision arithmetic.
Semantics follow SPARQL 1.1 sections 18.2.4 and 18.2.5:
https://www.w3.org/TR/sparql11-query/#convertSolMod
