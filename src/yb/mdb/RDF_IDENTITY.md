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

The adapter persists complete keys in rdf_idmap and reconstructs typed mappings
when a new process opens the keyspace. Legacy non-IRI entries without complete
identity are rejected with a reimport diagnostic. They cannot be reconstructed
reliably from their old packed IDs. Use a new keyspace and retain the old data;
there is no automatic destructive migration or rewrite of existing triples.

The storage format is independent of process pointers and registration order.
The in-process registry is a decoding cache, not the source of persistent term
identity. A missing mapping or conflicting identity is an error.

Existing arithmetic still has its original bounded integer/decimal precision.
Unrepresentable or ill-typed literals retain their datatype and lexical form
as typed literals rather than being coerced to simple strings. This change does
not implement all XSD arithmetic, date/time functions, full RDF/XML parsing,
query-wide memory budgets, or distributed worker execution.
The existing callback/CLI interface still returns lexical strings for literals;
LANG and DATATYPE expose their qualifiers inside SPARQL. It is not a typed
SPARQL JSON results serializer. Blank-node output labels include the packed ID
so distinct import scopes are not printed with the same label.

Regressions include MdbRdfIdentityTest, NtParserTest, and
MdbDistributedIntegrationTest.TypedIdentitySurvivesFreshCliProcesses. The last
test starts a fresh CLI process for each import/query, uses one-row pages, and
checks dictionary restoration, language/datatype expressions, numeric lexical
identity, blank-node scopes, and INSERT DATA persistence.
