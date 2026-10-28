# RDF term identity

New imports and INSERT DATA preserve the term kind, lexical form, and datatype
IRI or normalized language tag. Simple strings and explicit xsd:string use the
same identity. Numeric value conversion is separate from sameTerm: integer
lexical forms `01` and `1` remain different RDF terms.

Non-IRI dictionary keys use a versioned prefix and a length-delimited qualifier;
they do not concatenate values with a separator that may occur in the value.
IRI dictionary keys retain the old format to preserve existing IRI IDs. Packed
term hashes are checked for collisions instead of silently aliasing terms.

Each file import or INSERT DATA request assigns a fresh UUID scope to its blank
node labels. Repeated labels within that request refer to the same node; equal
labels in separate imports do not. The scope is included in the persisted key.

The authoritative dictionary is rdf_terms: complete term keys have immutable
ID claims, allocated by a conditional-write counter. Competing clients converge
on one ID per term; interrupted allocations may leave gaps. Initialization and
restoration reject conflicting IDs and a counter below its persisted claims.
Exhaustion fails instead of wrapping or reusing an ID.

rdf_idmap remains a compatibility mirror and a migration source for old data.
Importers persist term claims before committing their triples, so recovery from
rdf_terms does not depend on a final mirror flush. Existing legacy non-IRI
entries without complete identity are rejected with a reimport diagnostic.
They cannot be reconstructed reliably from their old packed IDs. Use a new
keyspace and retain the old data; there is no destructive triple migration.

Public transactional queries pin a query-owned data read point, refresh the
immutable dictionary, then bind query constants. They use the same snapshot for
adjacent scans even when the streaming flag is off; join-policy defaults are
unchanged. This also avoids stale process-wide data caches after another writer
commits. Nontransactional operation does not provide this snapshot guarantee.
Dictionary refresh currently scans all claims; its cost must be included in
performance measurements. A future cache shortcut cannot rely on the counter
alone: a writer may reserve an ID and publish its claim later.

File imports commit batches, not the entire file atomically. Restart recovery
preserves committed batches and permits retrying IRI/literal triples without
duplicating identical triples. Separate blank-node import scopes still differ.

The storage format is independent of process pointers and registration order.
The in-process registry is a decoding cache, not the source of persistent term
identity. A missing mapping or conflicting identity is an error.

Existing arithmetic still has its original bounded integer/decimal precision.
Unrepresentable or ill-typed literals retain their datatype and lexical form
as typed literals rather than being coerced to simple strings. This change does
not implement all XSD arithmetic, date/time functions, full RDF/XML parsing,
query-wide memory budgets, or distributed worker execution.
The structured result API and CLI JSON mode preserve datatype, language,
row boundaries, unbound cells, ASK booleans, and CONSTRUCT graphs. The legacy
scalar callback still has representation limits; see adapter/RESULTS.md.
Blank-node output labels distinguish separate import scopes.

Regressions include MdbRdfIdentityTest, NtParserTest, and
MdbDistributedIntegrationTest.TypedIdentitySurvivesFreshCliProcesses. The last
test starts a fresh CLI process for each import/query, uses one-row pages, and
checks dictionary restoration, language/datatype expressions, numeric lexical
identity, blank-node scopes, and INSERT DATA persistence.

MdbDictionaryRecoveryTest adds independent-client allocation, reservation gaps,
exhaustion, duplicate claims, rewound counters, recovery without the mirror,
SIGKILL after data commit, and a long-lived adapter reading another importer.
AuthenticatedSshTransportAndHostKeyRejection accepts MDB_TEST_SSHD for an
external test daemon; strict host-key verification remains enabled.
