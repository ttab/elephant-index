# Changelog

Everything from v1.4.0 onwards is documented here; earlier releases are not
reconstructed. The entries are derived from the release tags, and the linked
pull requests hold the detail.

## [v1.4.0] - Unreleased

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

**Deploy order (requires a Connect-serving repository):** this service now
calls the elephant repository with Connect clients, so **the repository has to
be deployed with Connect before this release goes out** — v1.9.0-pre2 or
later. Against an older repository every call is answered `unimplemented` with
a `404`, and since the schemas are loaded at startup the service does not come
up at all. There is no flag to fall back to the Twirp clients; the ordering is
the mitigation. Nothing else about the calls changes: the same endpoint
configuration, the same scopes, and the same token in the same place.

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

**Build (Go 1.27.1):** the module's `go` directive is `1.27.1`, up from
`1.26.4`. A build box pinned to an older toolchain fails on the upgrade rather
than falling back, which `GOTOOLCHAIN=auto` handles by downloading it and
`GOTOOLCHAIN=local` does not. The container image builds on
`golang:1.27.1-alpine3.24` and ships on `alpine:3.24`.

Changes:

- `--opensearch-endpoint` registers a cluster and creates a first index set
  again. The parsed URL was being assigned to a variable shadowed inside an
  `if`, so it never reached the setup code and the flag was silently ignored on
  every fresh installation; the credentials in its userinfo were read
  correctly, which is why it looked like it worked. **An installation that
  already has a cluster is unaffected** — the setup locks the cluster table and
  does nothing when one exists — so this only changes what happens on an empty
  database. Credentials given in the endpoint select basic authentication over
  IAM signing, and are moved out of the URL before the cluster row is written.
- Both RPC services are mounted on the Twirp and the Connect paths from one
  `elephantine.ServiceOptions`, so authentication, logging and metrics are
  identical on the two stacks by construction.
- No handler constructs a Twirp error any more: they return `*connect.Error`
  through the `elephantine/rpc` helpers, and a Twirp caller is answered by
  translating on the way out. Every message and every metadata key a caller
  reads is unchanged, apart from the `invalid_argument` above, and that is
  tested rather than asserted — the same failing call is made on both stacks
  and the code, message and metadata compared, with two error bodies per stack
  pinned by golden files.
- The repository clients are Connect clients, so this service no longer speaks
  Twirp to anything. `twitchtv/twirp` is gone from every source file here and
  is an indirect dependency only, kept by the generated Twirp server this
  service still mounts. The caller-token forwarding that `GetFlatDocument` and
  document loading depend on moved from `twirp.WithHTTPRequestHeaders` to
  `rpc.WithOutgoingHeaders` plus a `rpc.PropagateHeaders()` interceptor.
- `rpc_protocol_responses_total{service,method,protocol,code,client_id}` is
  reported by both stacks. `protocol="twirp"` falling to zero for a method is
  what says its Twirp mount can be retired, and `client_id` names the
  applications that still have to move.
- The test suite runs against either stack, selected by `TEST_RPC_STACK`, and
  CI runs it over both. Golden files record a success body and two error
  bodies per stack, so a change in either encoding is a visible diff.
- The job lock table is declared as vendored from elephantine. This service
  created `job_lock` by hand years before the library shipped a migration for
  it, so `schema/vendor.json` declares the library and the original migration
  asserts coverage; **no migration has to run for this release**, and CI now
  fails if a future elephantine migration is not taken.
- Dependency upgrades: Go to 1.27.1, elephantine to v0.29.0, elephant-api to
  the release carrying the `indexconnect` package, revisor to v1.0.3 (which
  takes a rewritten `gobwas/glob` matching engine, verified against this
  service's own field-filter patterns), mage to v0.14.0, eltest to v0.4.2, and
  the AWS SDK, Prometheus and `golang.org/x` sets. The generated database code
  is regenerated with sqlc v1.31.1, which changes only its version stamp.
- The repository gained this changelog.
