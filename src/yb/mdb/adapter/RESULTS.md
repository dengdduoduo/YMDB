# MDB result callbacks and CLI formats

`YBCMdbExecuteSparqlWithResults` reports a result kind and a column schema,
then complete rows, a Boolean, or graph triples. Every SELECT solution emits
one row, including zero-column and entirely unbound solutions. Each term
distinguishes an IRI, blank node, literal or unbound value; literals carry
their language or datatype. Column names preserve the original query names.

All callback pointers are borrowed for the duration of the call. Callbacks
run synchronously and must not re-enter the adapter. Output is provisional
until execution returns zero; callers must discard it on any nonzero return.
Successful INSERT DATA retains its existing status-only behavior.

The CLI stages results in a bounded temporary file before publishing them.
Failures leave stdout empty. `--mdb_result_format=json` emits SPARQL Results
JSON for SELECT and ASK. Unbound variables are omitted from each binding
object; an empty solution is `{}`, distinct from zero solutions. CONSTRUCT
uses a graph envelope with `type: graph` and a `triples` array of three typed
term objects per triple. This graph envelope is an MDB format, not SPARQL
Results JSON. In text mode, graph results use N-Triples.

The default SELECT text mode keeps the existing internal column labels for
compatibility and prints UNDEF for unbound cells. Use JSON or typed callbacks
when variable names and RDF term identities must be reconstructed without
ambiguity. The legacy scalar callback remains available for lexical-value
consumers, but cannot express empty solutions or typed terms. It explicitly
rejects zero-column/all-unbound rows rather than silently dropping them.

Residual standard property paths use the query's transactional paged source,
including the CLI's default read mode. Their intermediate state/work is
bounded, and they observe query cancellation and deadlines. They currently
execute at the coordinator when fragment planning cannot distribute them.
This is correctness support, not a distributed-path performance claim.
