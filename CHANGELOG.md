# Changelog

Everything from v1.4.0 onwards is documented here; earlier releases are not
reconstructed. The entries are derived from the release tags, and the linked
pull requests hold the detail.

## [v1.4.2] - Unreleased

**New API surface (a hit says what it is):** every `HitV1` in a `Query` or
`MultiSearch` response now carries `document_type`. A query that spans several
document types returns a mixed result set, and nothing on a hit told them
apart before — a caller either inferred the type from a field it had indexed
itself, or ran one query per type so that the type came from the query rather
than the answer. It needs elephant-api v0.25.2 or later.

The type is derived from the index a hit came from, not stored on the
document, so **it is correct for everything already indexed and nothing has to
be re-indexed for it**. An index name on its own is not enough — sanitizing a
document type for use in an index name maps `/`, `+` and spaces all onto `_` —
so the value is read from the `document_index` registry, which has recorded
the unsanitized type since the index was created.

**Behaviour change (search reads the database):** serving a search now
involves a Postgres lookup, where it previously needed only OpenSearch. The
result is cached per index name for an hour, because an index's document type
is fixed when the index is created, so the query rate against the database is
roughly one statement per index per hour and not one per search. A replica
that cannot reach Postgres now fails searches that it would previously have
served. Operators watching this service's database dependency should know that
the read path, not just subscriptions and the management API, now depends on
it.

Changes:

- `HitV1` gains `document_type`, resolved from the `document_index` registry
  and cached per index name.
- New query `GetIndexContentTypes`, which is the registry lookup behind it.
- Dependency upgrades: elephant-api to v0.25.2.

## [v1.4.1] - 2026-09-17

**New API surface (multi-type queries):** `SearchV1.Query` and
`SearchV1.MultiSearch` accept a repeated `document_types` on
`QueryRequestV1`, naming the document types a query should span. A query that
spans several types searches several indices, which is what the field is for —
`document_type` names one type and has not changed, and a query that names no
type at all still searches every type. The two fields are unioned rather than
exclusive, so `document_type: "core/article"` together with
`document_types: ["core/planning-item"]` searches both, and a client can adopt
the plural field without first removing the singular one. It needs
elephant-api v0.25.1 or later.

Two limits come with it, and this service enforces neither. Document types are
indexed separately so that their mappings can differ, and a field name that
carries different types in different document types cannot be queried or
sorted on across them — OpenSearch rejects the whole search, not the offending
index. `GetMappings` still answers for a single document type, so reconciling
the mappings of several types before building such a query is the caller's
job. A query may name at most 50 types; past that it is refused as an invalid
argument, because the index list travels in the request path.

Subscriptions stay single-type: a percolator is registered for one document
type, so `subscribe` together with more than one type is refused as an invalid
argument. The one type may come from either field.

**Behaviour change (a document type with no index):** a query that names a
document type with nothing indexed in the active set now returns no hits for
it instead of failing. Searches are issued with `ignore_unavailable`, so a
named index that does not exist is skipped. Previously such a query answered
`internal` with the text of OpenSearch's `index_not_found_exception`, which
this reached whenever the type and a region-qualified language named an index
concretely rather than as a wildcard — a fresh index set, a type nothing has
been written for yet, a typo. It is now indistinguishable from a type that
exists and matches nothing. Anything that treated that error as "this type is
not indexed yet" has to look at the index set instead.

Changes:

- `QueryRequestV1` gains `document_types`, and `IndexPattern` builds a
  comma-separated index list from it, collapsing types that sanitize to the
  same index name.
- Searches and multi searches set `ignore_unavailable`.
- `MultiSearch` validates each query before building its metadata line, so a
  refused query no longer has an index list built for it first.
- Dependency upgrades: elephant-api to v0.25.1 and ttab/mage to v0.15.0.

## [v1.4.0] - 2026-09-16

**New API surface (Connect):** every method of both services is now served on
a second path family, `POST /elephant.index.<Service>/<Method>`, alongside the
existing `POST /twirp/elephant.index.<Service>/<Method>`. **Twirp is
unchanged** — same paths, same bodies, same codes — and stays until this
service's next major release, so no caller has to move. The Connect paths
serve the Connect protocol from outside the cluster, and gRPC and gRPC-Web on
the same paths inside it; the fleet's ingress speaks HTTP/1.1, so gRPC is not
reachable externally and is not offered.

Three things differ on the wire, and a caller that reads JSON by hand with
`fetch` or `curl` is the one they reach. A generated client is unaffected.

* **JSON field names.** A Connect response spells its fields in
  lowerCamelCase (`indexSets`), where a Twirp response spells them as the
  `.proto` declares them (`index_sets`). Requests accept either spelling on
  both stacks. A caller that changes only the path prefix gets a `200` and
  reads `undefined` for every multi-word field.
* **Error bodies.** Twirp renders `{"code":…,"msg":…,"meta":{…}}`; Connect
  renders `{"code":…,"message":…,"details":[…]}` with the metadata carried as
  an `elephantine.rpc.ErrorMeta` detail. The 16 codes are spelled identically.
* **HTTP status for three codes.** `failed_precondition` is `400` on Connect
  where Twirp answered `412`, `canceled` is `499` against `408`, and
  `deadline_exceeded` is `504` against `408`. Read the code from the body
  rather than the status.

**Behaviour change (an invalid token is answered 401):** inherited from
elephantine, and it applies to both stacks and every path behind the
authentication middleware. A token that cannot be authenticated is now
answered `unauthenticated` (`401`) where it was answered `permission_denied`
(`403`); `403` is left to mean a caller we did identify that lacks a scope.
Anything keyed on `403` for a bad token — an ingress rule, a dashboard panel,
a client's retry logic — reads `401` after the upgrade.

**Behaviour change (a malformed query is answered invalid_argument):** a
`Query` or `MultiSearch` whose query cannot be translated — in practice an
unsupported query type — is answered `invalid_argument` where it was answered
`internal`. The handler was recoding the request parser's error, so the
parser's own `invalid_argument` never reached the caller. The validation
failures that were already reported as `invalid_argument`, such as pagination
combined with a subscription, are unchanged. Anything treating an `internal`
from this API as "retry, the service is broken" should read the code again: a
query it will never accept now says so.

**Behaviour change (long-poll timeouts):** `PollSubscription` answers a call
whose deadline passed while it was waiting with `deadline_exceeded`, and one
whose caller went away with `canceled`, where both previously produced an
empty successful response. This matters to a Connect caller, because Connect
turns a `Connect-Timeout-Ms` header into the handler's deadline and enforces
it: a caller that asks for a timeout shorter than `max_wait_ms` now reads a
timeout instead of an empty result, and the server records it as one. A poll
that reaches its own `max_wait_ms` with nothing to report still returns an
empty successful response, which is the ordinary idle case and is unchanged on
both stacks. Twirp ignores client deadlines, so only a disconnect reaches this
there.

**Behaviour change (request bodies are capped):** inherited from elephantine,
which caps request bodies at 8 MiB where they were previously unbounded. A
request declaring a larger `Content-Length` is refused with a plain `413` on
both stacks. A chunked request, or one that lies about its length, fails on
the read that passes the limit, and there the stacks differ: Twirp answers
`malformed` with `400` and Connect answers `resource_exhausted` with `429`.
A large `MultiSearch` is the request in this API most likely to notice.

**Behaviour change (a failing bulk index is retried, not skipped):** the
indexer now treats a transient OpenSearch failure as retryable and blocks at
the same event-log position until it succeeds. Any `5xx` or a `429` counts,
including the `503 unavailable_shards_exception` returned while a primary
shard is unassigned, and a bulk request rejected as a whole is caught as well
as an item that failed inside one. Previously the position advanced anyway and
those documents stayed missing from the index until something replayed them,
which is what a brief loss of an OpenSearch data node produced. A cluster in
trouble now shows as indexing lag that stops advancing, with
`elephant_indexer_failures_total` climbing, rather than as a silently
incomplete index, so that is what to alert on. Failures the document itself
causes — a `4xx`, a mapping conflict — would fail identically on retry and are
still skipped, so one bad document cannot wedge the consumer.

**Build (Go 1.27.1):** the module's `go` directive is `1.27.1`, up from
`1.26.4`. A build box pinned to an older toolchain fails on the upgrade rather
than falling back, which `GOTOOLCHAIN=auto` handles by downloading it and
`GOTOOLCHAIN=local` does not. The container image builds on
`golang:1.27.1-alpine3.24` and ships on `alpine:3.24`.

Changes:

- `SearchV1.GetFlatDocument` returns a single document in the flattened
  property representation the indexer builds, as a map of field name to
  values, alongside the document itself. By default it fetches the current
  version from the repository and flattens it on the spot, which bypasses the
  indexing lag: a document written a moment ago can be inspected before it is
  searchable. With `stored` set it returns what the active index actually
  holds, which is what answers "why does this document not match my query".
  The type and the language are taken from the document, so neither is a
  request field; `version` and `status` select which version to flatten. It
  takes `search` or `index_admin` like the rest of `SearchV1`, and the
  repository read is made with **the caller's own token**, so it cannot reach
  a document the caller is not allowed to read. It is also the one search
  method that fails when the repository is down. (#291)
- `--opensearch-endpoint` registers a cluster and creates a first index set
  again. The parsed URL was being assigned to a variable shadowed inside an
  `if`, so it never reached the setup code and the flag was silently ignored on
  every fresh installation; the credentials in its userinfo were read
  correctly, which is why it looked like it worked. **An installation that
  already has a cluster is unaffected** — the setup locks the cluster table and
  does nothing when one exists — so this only changes what happens on an empty
  database. Credentials given in the endpoint select basic authentication over
  IAM signing, and are moved out of the URL before the cluster row is written.
  (#297)
- Transient bulk index failures are retried from the same event-log position
  rather than skipped, as described above. A whole-request rejection is caught
  as well as a failure on an individual item, the counters a partial batch did
  report are still emitted before it is retried, and the log line for a failed
  item says whether it was judged retryable. (#284)
- Both RPC services are mounted on the Twirp and the Connect paths from one
  `elephantine.ServiceOptions`, so authentication, logging and metrics are
  identical on the two stacks by construction. (#297)
- No handler constructs a Twirp error any more: they return `*connect.Error`
  through the `elephantine/rpc` helpers, and a Twirp caller is answered by
  translating on the way out. Every message and every metadata key a caller
  reads is unchanged, apart from the `invalid_argument` above, and that is
  tested rather than asserted — the same failing call is made on both stacks
  and the code, message and metadata compared, with two error bodies per stack
  pinned by golden files. (#297)
- `rpc_protocol_responses_total{service,method,protocol,code,client_id}` is
  reported by both stacks. `protocol="twirp"` falling to zero for a method is
  what says its Twirp mount can be retired, and `client_id` names the
  applications that still have to move. (#297)
- The test suite runs against either stack, selected by `TEST_RPC_STACK`, and
  CI runs it over both. Golden files record a success body and two error
  bodies per stack, so a change in either encoding is a visible diff. (#297)
- The job lock table is declared as vendored from elephantine. This service
  created `job_lock` by hand years before the library shipped a migration for
  it, so `schema/vendor.json` declares the library and the original migration
  asserts coverage; **no migration has to run for this release**, and CI now
  fails if a future elephantine migration is not taken. (#297)
- Dependency upgrades: Go to 1.27.1, elephantine to v0.29.0, elephant-api to
  v0.25.0 (which carries the `indexconnect` package), revisor to v1.0.3 (which
  takes a rewritten `gobwas/glob` matching engine, verified against this
  service's own field-filter patterns), newsdoc to v1.1.0, revisorschemas to
  v1.5.3, mage to v0.14.0, eltest to v0.5.0, pgx to v5.10.0, tern to v2.4.3,
  `urfave/cli` to v3.11.0, and the AWS SDK, Prometheus and `golang.org/x`
  sets. `golang.org/x/exp` is dropped for the standard library's `slices` and
  `maps`. The generated database code is regenerated with sqlc v1.31.1, which
  changes only its version stamp. (#284, #291, #297)
- The repository gained this changelog, and a documentation set alongside it:
  `docs/architecture.md`, `docs/ops.md` and `docs/observability.md`, with the
  README reorganised around them. (#297)
