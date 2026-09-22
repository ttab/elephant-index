# Changelog

Everything from v1.0.0 onwards is documented here; earlier releases are not
reconstructed. The entries are derived from the release tags, and the linked
pull requests hold the detail.

## [v1.5.0] - Unreleased

**Deploy order (requires a Connect-serving repository):** this service now
calls the elephant repository with Connect clients, so **the repository has to
be deployed with Connect before this release goes out** — v1.9.0 or later.
Against an older repository every call is answered `unimplemented` with a
`404`, and since the schemas are loaded at startup the service does not come up
at all. There is no flag to fall back to the Twirp clients; the ordering is
the mitigation. Nothing else about the calls changes: the same endpoint
configuration, the same scopes, and the same token in the same place.

**Behaviour change (a failed refresh stalls percolation):** making a
percolator query visible to search is now part of percolating an event, and an
error doing it is returned rather than logged and skipped. The percolation
loop retries the event from its last position, so a cluster that persistently
refuses the refresh stalls percolation for the whole index set, where it
previously degraded only the one subscription whose query never became
visible. The trade is deliberate — reporting a matching document as a
non-match is worse than reporting it late — but it is a new way for
`elephant_indexer_percolator_position` to go flat, and the runbook names it.

Changes:

- A crash that could kill a replica is fixed. The in-memory cache of language
  settings was unguarded, so two goroutines resolving a language neither had
  seen before could abort the process with `fatal error: concurrent map
  writes`. That is a runtime throw rather than a panic, so nothing recovered
  it — the process exited and the pod restarted, taking search, indexing and
  subscription delivery on that replica with it. Two paths reached it: a
  re-index, where two index sets follow the event log side by side and both
  indexers shared one cache, which is how two production replicas died within
  fifteen minutes of an index set activation; and the percolator, which
  resolves subscription languages from two of its own goroutines and so needed
  neither a re-index nor a second index set. The cache is now safe for
  concurrent use by construction. (#306)
- A new subscription no longer reports matching documents as non-matches for
  the first second of its life. A percolator query is an OpenSearch document
  and is only matched once a refresh has made it visible; the code refreshed
  the index with a flush, which makes the write durable without making it
  visible, and the path that registers a subscription did not refresh at all.
  Every write of a percolator query now refreshes before anything percolates
  against it, and a new subscription is registered only once its query is
  evaluable, so percolation never sees the state in between. The set of
  percolators a document is reported against is likewise read before the
  percolate search rather than after it, which closes the same wrong answer in
  a window one search round trip wide. A document indexed before the
  subscription is registered is still not matched against it — that is a
  missed notification, which the delivery contract allows, rather than a wrong
  answer. (#299)
- Registering a subscription no longer stalls percolation while its query is
  written. The percolator held the lock that percolation needs across a
  Postgres transaction and the OpenSearch write, so every new subscription
  blocked matching for the duration; the lock is now taken for the
  registration itself and nothing else. (#299)
- The repository clients are Connect clients, so this service no longer speaks
  Twirp to anything. `twitchtv/twirp` is gone from every source file here and
  is an indirect dependency only, kept by the generated Twirp server this
  service still mounts. The caller-token forwarding that `GetFlatDocument` and
  document loading depend on moved from `twirp.WithHTTPRequestHeaders` to
  `rpc.WithOutgoingHeaders` plus a `rpc.PropagateHeaders()` interceptor. (#298)

## [v1.4.2] - 2026-09-17

**New API surface (a hit says what it is):** every `HitV1` in a `Query` or
`MultiSearch` response now carries `document_type`. A query that spans several
document types returns a mixed result set, and nothing on a hit told them
apart before — a caller either inferred the type from a field it had indexed
itself, or ran one query per type so that the type came from the query rather
than the answer. It needs elephant-api v0.25.2 or later.

The type is derived from the index a hit came from, not stored on the
document, so **it is correct for everything already indexed and nothing has to
be re-indexed for it**. An index name on its own is not enough — sanitizing a
document type for use in an index name maps `/`, `+` and spaces all onto `_`
— so the value is read from the `document_index` registry, which has
recorded the unsanitized type since the index was created.

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

## [v1.3.2] - 2026-05-22

**Behaviour change (an unhealthy cluster no longer fails readiness):**
the OpenSearch check on `/health/ready` is now optional, so a replica whose
active cluster is unreachable or unhealthy stays ready and keeps serving. It
was a hard readiness function before, which meant a cluster wobble rolled the
whole deployment out of the load balancer at once — including the replicas
that were only proxying or answering subscription polls, neither of which
needs the cluster to be healthy. The check still runs and still reports, so
`/health/ready` output names it; what changed is that it no longer gates the
endpoint's status. Anything alerting on pods leaving readiness during an
OpenSearch incident has to watch the check itself, or the indexing lag,
instead.

Changes:

- The readiness request to OpenSearch is given its own 500 ms timeout, so a
  cluster that accepts connections and then hangs cannot hold a readiness
  probe open until the probe's own deadline.
- Dependency upgrades: Go to 1.26.3 (image `golang:1.26.3-alpine3.23`),
  elephantine to v0.26.2, elephant-api to v0.22.4, revisor to v1.0.0,
  revisorschemas to v1.5.0, tern to v2.4.1, mage to v0.9.1 and the AWS SDK
  suite. revisorschemas v1.5.0 renames its schema files to reverse-domain
  style (`core.json` to `se.ecms.json`, and so on) and restructures the eidos
  block, which reaches this repository only as test fixtures.
- The GitHub Actions build and lint workflows are updated.

## [v1.3.1] - 2026-04-29

**New configuration (serving TLS directly):** `--tls-addr`/`TLS_ADDR`,
`--cert-file`/`TLS_CERT_PATH` and `--key-file`/`TLS_KEY_PATH` make the service
serve HTTPS on a second listener, defaulting to `:1443`. Nothing happens
unless `--cert-file` is set, so an installation that terminates TLS at the
ingress is unaffected.

Changes:

- Dependency upgrades: elephantine to v0.26.1.

## [v1.3.0] - 2026-04-13

**Behaviour change (fuzzy search actually fuzzes):** `fuzziness` and
`prefix_length` on `MultiMatchQueryV1` are translated into the OpenSearch
`multi_match` query. Both fields existed on the request and neither reached
OpenSearch, so a caller asking for fuzzy matching silently got exact matching;
those queries now return more hits, and in a different order. A client that
has been compensating for the old behaviour — widening the query itself, or
sending a fuzziness it knew was ignored — sees the difference first. It needs
elephant-api v0.22.1 or later.

`fuzziness` carries either an edit distance or an `auto` sub-message. `auto`
with no thresholds becomes OpenSearch's `AUTO`, and `auto` with a low or high
term length becomes `AUTO:low,high`; an edit distance is passed through as the
integer. (#274)

Changes:

- Dependency upgrades: elephant-api to v0.22.1.

## [v1.2.6] - 2026-03-06

**Breaking (index names change for document types with a variant):** a
document type may now carry a `#` variant suffix, as in
`core/article#template`, and the variant is separated by `--` in the index
name rather than collapsed into `_`: `core/article#template` indexes into
`documents-<set>-core_article--template-<language>` where it previously
indexed into `documents-<set>-core_article_template-<language>`. Sanitizing
maps every other non-alphanumeric character onto `_`, so without the separate
treatment `core/article#template` and a hypothetical `core/article_template`
share one index and mix unrelated documents into one mapping.

**A document type whose index name changes loses everything already indexed
under it.** The indexer writes to the new name and creates the index on
demand, the old index keeps the documents and nothing deletes it, and a query
builds its index pattern with the same sanitizer — so it looks at the new,
nearly empty index and the old documents are simply not found. A query that
names the type without a region-qualified language builds a wildcard pattern,
which matches the new index and nothing else: it returns far fewer hits than
it should, with no error and nothing logged. One that pins a region-qualified
language names the index concretely and is answered `internal` with
OpenSearch's `index_not_found_exception` until the first document lands under
the new name. Types with no `#` in them are spelled exactly as before and are
unaffected, so the blast radius is the set of types carrying a variant suffix.
**If any of those have been indexed, re-index into a new index set and cut
over** — the mapping cannot be moved in place, and there is no flag that keeps
the old names. Both the indexing path and the index pattern a query is turned
into use the same sanitizer, so the two agree with each other either way; what
they cannot do is agree with what an earlier release wrote. (#255, #264)

**Behaviour change (block field names have a fallback):** a meta or content
block with no `type`, or a link block with no `rel`, used to flatten to a
field name with an empty key — `meta.` — which put unrelated blocks in one
bucket. The key now falls back through `type`, `rel`, `role` and `name`, and
the value is prefixed with the attribute it came from, so a link with no `rel`
but a `type` of `text/html` becomes `rel.type__text_html`; with nothing to
fall back on the key is `__unknown`. These are new field names, so they need
new mappings, and **the blocks that were indexed under the old empty key stay
that way until they are re-indexed into a new index set**. (#256)

**Build (Go 1.26.0, Alpine 3.23):** the image builds on
`golang:1.26.0-alpine3.23` and ships on `alpine:3.23`, up from Go 1.25.4 and
Alpine 3.22.

Changes:

- A CA certificate registered on a cluster is appended to the system
  certificate pool instead of replacing it. `ca_cert` previously became the
  client's entire set of roots, so registering a cluster's private CA broke
  verification of every publicly signed endpoint the same client reached.
  (#263)
- `scripts/set-encryption-key` generates a password encryption key, and the
  README documents what the key is for and that it cannot be rotated.
- Dependency upgrades: Go to 1.26.0, elephantine to v0.25.0, elephant-api to
  v0.21.3, revisor to v0.11.1, revisorschemas to v1.2.0-pre6, `urfave/cli`
  from v2 to v3, golangci-lint to v2.9, and the AWS SDK suite.

## [v1.2.5] - 2025-11-25

**New API surface (TLS options on a cluster):** `RegisterCluster` accepts
`auth.ca_cert`, a PEM bundle to verify the cluster's certificate against, and
`auth.insecure_tls`, which turns verification off entirely. A `ca_cert` that
is not a PEM bundle of `CERTIFICATE` blocks is refused as an invalid argument
rather than failing later, when a client is built. It needs elephant-api
v0.19.3 or later.

Changes:

- The OpenSearch HTTP client is given explicit connection settings: a 3 s TLS
  handshake timeout, 10 idle and 10 total connections per host, and a 90 s
  idle connection timeout.
- Dependency upgrades: elephantine to v0.22.1 and elephant-api to v0.19.3.

## [v1.2.4] - 2025-11-24

**Behaviour change (searching before there is an index set):** `Query` answers
`failed_precondition` with "no active index" when no index set is active,
where it previously panicked on a nil client and the caller read a `500`.
This is the state a fresh installation is in between coming up and having its
first index set activated.

Changes:

- Activating an index set on an installation that has none no longer fails.
  The check for a currently active set treated "no rows" as an error, so the
  first activation could not complete.

## [v1.2.3] - 2025-11-24

**Behaviour change (readiness before there is an index set):**
`/health/ready` passes when no index set is active, where the OpenSearch check
previously failed on the missing row and held a fresh installation out of the
load balancer — which also kept the management API it needs to register a
cluster and create the first set out of reach.

## [v1.2.2] - 2025-11-24

**Breaking (a password encryption key is required):** `--password-key`, from
`PASSWORD_ENCRYPTION_KEY`, is a required flag and the service does not start
without it. It is a 32-byte hex-encoded key, and it encrypts the cluster
passwords stored in the database. **Nothing re-encrypts stored passwords, so
the key cannot be rotated**, and a wrong key is not detected at startup but
when a client for the cluster is built. Generate one and put it in the
deployment before this release goes out.

**New API surface (username and password on a cluster):** `RegisterCluster`
accepts `auth.username` and `auth.password` alongside `auth.iam`, and refuses
the two together as an invalid argument. `ListClusters` reports the username,
never the password. It needs elephant-api v0.19.2 or later.

**Behaviour change (`--opensearch-endpoint` is optional):** the service starts
with no cluster registered, so an installation can be brought up and its
cluster registered over the API instead of passing credentials in the
environment. Credentials given in the endpoint's userinfo select password
authentication over IAM signing and are moved out of the URL before the
cluster row is written, so the stored row carries no secret.

## [v1.2.1] - 2025-11-14

Test infrastructure only: the suite starts its backing services in a way that
works with Docker Desktop on macOS, and brings up its own OIDC container.
Nothing here changes what the service does. (#231)

## [v1.2.0] - 2025-11-11

**Behaviour change (a panic no longer takes the process down):** the indexer,
the cleanup loop and the server goroutines run through elephantine's
`ErrGroup` and `CallWithRecover`, so a panic in one of them is recovered,
logged and turned into an error for the supervising group rather than
unwinding the whole process. A panicking indexer now looks like an indexer
that stopped — its job lock is released and another replica picks the index
set up — instead of a crash loop across every replica. (#223)

**Build (Go 1.25.4):** the module's `go` directive is `1.25.4`, up from
`1.24.6`, and the image builds on `golang:1.25.4-alpine3.22`. (#229, #230)

Changes:

- The repository clients are built with elephantine's HTTP client helpers, so
  the long-polling event log client gets the timeouts meant for long polls
  rather than the ten-second response header timeout the hand-rolled client
  used.
- Dependency upgrades: Go to 1.25.4, elephantine to v0.22.0, elephant-api to
  v0.18.2, revisorschemas to v1.0.7, eltest to v0.2.1, howdah to v0.0.3 and
  the AWS SDK suite. (#229)

## [v1.1.3] - 2025-10-01

Changes:

- The subscription cache evicts expired entries every ten seconds instead of
  only when it runs out of room, so a long-running replica releases the memory
  of subscriptions nobody has polled for half an hour.

## [v1.1.2] - 2025-09-30

Changes:

- The percolator document cache is cut from 5 000 entries held for an hour to
  500 held for ten minutes, with eviction running every ten seconds. It caches
  the flattened documents a poll response reads, which are already in Postgres
  and expire there after 90 minutes, so the cache was holding far more, for
  far longer, than a poll can use. Replica memory drops; a poll for an
  evicted document reads it from the database instead. (#214)

## [v1.1.1] - 2025-09-22

**Behaviour change (a delete no longer stops the indexer):** an event log
batch containing a delete crashed the indexer on a nil dereference when
percolation was enabled, which took every index set on that replica down with
it and left the batch to be replayed into the same crash. Deletes are now
skipped when the batch is queued for percolation, so **a delete is not
percolated at all** and a subscription is not notified of one. That is a
missed notification, which the delivery contract allows, in place of a
stalled indexer. (#209)

## [v1.1.0] - 2025-09-16

**New API surface (`MultiSearch`):** `SearchV1.MultiSearch` runs several
queries in one request, through OpenSearch's msearch, and returns a response
per query in the order they were given. It takes the same scopes and builds
each query exactly as `Query` does — the request translation moved into a
shared `internal` package so that the two cannot drift. It needs elephant-api
v0.18.1 or later. (#191)

**Behaviour change (a percolator is per document type and language):** a
subscription's percolator is now registered for a language as well as a
document type, and the uniqueness of a stored percolator is keyed on
`(doc_type, language, hash, owner)` rather than `(hash, owner)`. Two
subscriptions with the same query text against different languages used to
share one percolator and one set of notifications. **Percolators that existed
before the upgrade are migrated with an empty language**, which does not match
any subscription registered afterwards, so they are replaced as clients
resubscribe and cleaned up once nothing references them. A subscription is
short-lived, so this settles within the half hour a subscription lives without
being polled. (#207)

**Migrations:**

- `schema/005_percolation_language.sql` adds `percolator.language` with a
  default of `''` and replaces the `pcl_unique_hash` constraint with one that
  includes `doc_type` and `language`. It must run **before** the deploy, needs
  no maintenance window, and takes no meaningful time — the table holds only
  live subscriptions' percolators. Note that the migration's rollback half is
  not valid SQL, so `mage sql:rollback` past it fails; a rollback has to drop
  the column and restore the constraint by hand.

Changes:

- Percolation events are queued once per indexed batch, in event order,
  instead of one at a time as each index worker finishes. Workers run
  concurrently per document type and language, so the events reached the
  percolator out of order and the percolator, which advances a single
  position, skipped everything that arrived behind that position. Under load
  this is a subscription silently missing documents. (#207)
- A percolator query is written to OpenSearch as soon as the percolator is
  registered, for a percolator with a known language, rather than lazily on
  the first document of that type. (#207)
- The README describes what happened upgrading OpenSearch in place: a
  blue/green v2.5 to v2.19 upgrade in stage lost documents and indices, so the
  practice is to stand up a new cluster, re-index into it and switch over —
  which is also reversible.
- Dependency upgrades: Go to 1.24.6, elephantine to v0.20.4, elephant-api to
  v0.18.1, revisor to v0.9.4 and revisorschemas to v1.0.5.

## [v1.0.10] - 2025-06-18

Changes:

- Dependency upgrades: Go to 1.24.4 (image `golang:1.24.4-alpine3.22`, shipped
  on `alpine:3.22`), elephantine to v0.19.2, pgx to v5.7.5, `urfave/cli` to
  v2.27.7 and the AWS SDK suite.

## [v1.0.9] - 2025-06-03

**Behaviour change (`doc_read_all` bypasses the readers filter):** a `Query`
from a client holding `doc_read_all` is no longer restricted to documents the
caller is named a reader of. Only `doc_admin` bypassed the filter before, so a
client with `doc_read_all` — which the repository honours as read access to
everything — got a silently narrowed result set from search. A shared query
still applies the restriction for both scopes, because a shared query's
results are seen by someone other than the caller.

## [v1.0.8] - 2025-05-20

Internal naming and comments only. Nothing here changes what the service does.

## [v1.0.7] - 2025-05-19

Changes:

- A subscription poll result no longer carries an item whose document could
  not be loaded. The load failure was logged and the item appended anyway,
  with an empty document, so a client read a notification about a document
  with no type, no language and no fields. Such an item is now left out, and
  the event is a missed notification rather than an empty one.

## [v1.0.6] - 2025-05-18

**Behaviour change (a percolator is per document type):** an existing
percolator is reused for a new subscription only when the document type
matches, as well as the query hash and the owner. Two subscriptions with the
same query text against different document types used to share one percolator,
so the second subscriber was notified about the first one's document type and
never about its own.

Changes:

- `elephant_indexer_percolator_lifecycle_total` gains the `query-doc` and
  `query-doc-error` events, counting percolator queries written to OpenSearch
  and the writes that failed.

## [v1.0.5] - 2025-05-18

**Behaviour change (percolation recovers from a lost notification):** the
percolation loop wakes on its own every five seconds as well as on a
notification, and percolates up to the last event id in the database rather
than up to the id the notification happened to carry. Notifications have no
delivery guarantee, so a lost one used to leave its events unpercolated until
another notification arrived to carry the position past them — and with
nothing being written, that could be indefinitely. Subscriptions now see those
documents within five seconds instead of not at all.

Changes:

- New metric `elephant_indexer_percolator_lifecycle_total{event}`, counting
  the percolation loop's own progress: `acquire-lock`, `start`, `stop`,
  `triggered`, `poll`, `no-work` and `end-iteration`. A loop that is running
  but finding nothing to do is `poll` and `no-work` climbing together, which
  is the ordinary idle shape and not a stall.

## [v1.0.4] - 2025-05-15

Changes:

- `elephant_indexer_percolator_position` is exported. It was created but never
  registered, so it never appeared in `/metrics` at all.
- `elephant_indexer_percolation_total{event="percolate-event"}` counts events
  the percolator has finished, not events it has picked up, so it can no
  longer run ahead of the position gauge.

## [v1.0.3] - 2025-05-15

Changes:

- `elephant_indexer_percolation_total` gains the `percolate-event` event,
  counted on the percolator's side of the queue with a `location` of
  `percolator`. The metric only reported what was queued before, so there was
  nothing to compare it against; the gap between `queued` and
  `percolate-event` is the backlog.

## [v1.0.2] - 2025-05-15

Changes:

- New metric `elephant_indexer_percolation_total{event,location}`, counting
  percolation events by what happened to them and where. `requested` is an
  indexed document offered for percolation and names the index it came from;
  `queued`, `queue_failed` and `inactive_set` are what the coordinator did
  with it and name the index set. `inactive_set` is routinely non-zero for the
  whole duration of a re-index and means nothing on its own — only the active
  index set's percolation requests are honoured.
- New metric `elephant_indexer_percolator_position`, the event log position
  the percolator has reached. This is the gauge to alert on for a stalled
  percolator: it is flat whenever percolation is not advancing, whatever the
  reason. It is not actually exported in this release — the collector was
  never registered, which v1.0.4 fixes.

## [v1.0.1] - 2025-05-10

Changes:

- Dependency upgrades: elephantine to v0.18.1.

## [v1.0.0] - 2025-05-10

The first release under v1. The service follows the repository event log,
flattens each document into a flat property structure and indexes it into
OpenSearch, serves search over what it has indexed plus a management API for
index sets and clusters, and matches newly indexed documents against stored
subscription queries so that clients can long-poll for changes.

Changes:

- `--cors-host`/`CORS_HOSTS` sets the origins the API answers CORS requests
  for, repeatable and supporting wildcards. It was not configurable before.
  (#176)
