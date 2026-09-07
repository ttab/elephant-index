# Elephant index — operations

For somebody holding a pager or triaging: what the service depends on, what it
listens on, what order it comes up in, and every failure mode with the signal
that shows it.

| Document | What it settles |
|---|---|
| [../README.md](../README.md) | Orientation and the working reference: layout, build, how to run it, every configuration flag. |
| [architecture.md](architecture.md) | How the service is built: process model, data flow, subsystems, API surface. |
| **ops.md** (this document) | Dependencies, ports, bootstrap order, failure modes and their signals. |
| [observability.md](observability.md) | Every metric and what a change in it means. |

This document is not a design reference — when a failure mode needs the "why",
it links to [architecture.md](architecture.md). It names metrics as signals;
[observability.md](observability.md) defines them.

## What the service is

Two halves in one process, and **they fail independently**:

* **The read half** — the search and management RPCs and the elastic proxy. It
  needs Postgres and the active OpenSearch cluster. It does not need the
  repository.
* **The write half** — the indexers and the percolator, following the
  repository event log into OpenSearch. It needs Postgres, OpenSearch and the
  repository.

A replica that cannot reach the repository still serves search against
whatever is already indexed; results simply go stale. That is the distinction
to hold on to while triaging: **stale results and no results have completely
different causes.**

## Components

| Repository | What it is to us |
|---|---|
| `ttab/elephant-index` | This service. |
| `ttab/elephant-index-deploy` | Its chart and environment configuration. |
| `ttab/elephant-repository` | The source of the event log, documents and revisor schemas. |
| `ttab/elephantine` | API server, authentication, job locks, RPC metrics, the `rpc` error vocabulary. |
| `ttab/koonkie` | The event log follower and its position metric. |
| `ttab/revisor` / `ttab/revisorschemas` | Schema validation, the input to mapping construction. |

## Runtime dependencies

| Dependency | Needed for | What happens without it |
|---|---|---|
| Postgres | Everything: index set registry, cluster registry, subscriptions, percolation state, job locks | Readiness fails and the replica leaves rotation. Nothing works. |
| OpenSearch (active cluster) | Serving search, indexing documents | Search fails; indexing retries and lags. Readiness stays up — the check is optional by design. |
| Elephant repository | Following the event log, loading documents, loading revisor schemas | Indexing stops and lags. Search keeps serving stale results. Startup fails if schemas cannot be loaded at all. |
| OIDC provider | Authenticating every RPC and proxy request | All requests are refused. Search and indexing are otherwise unaffected. |
| AWS IAM | Signing OpenSearch requests, only with `--managed-opensearch` | OpenSearch requests are refused. |

**Only Postgres is truly required to stay in rotation.** Everything else
degrades into lag or into a subset of the API failing, which is deliberate:
see [Failure modes](#failure-modes) for which is which.

## Endpoints and ports

| Port | Default | What is on it |
|---|---|---|
| API | `:1080` (`ADDR`) | Both RPC path families, the elastic proxy on `/`, and CORS. |
| Debug | `:1081` (`PROFILE_ADDR`) | `/metrics`, `/health/ready`, `/health/alive`, pprof. |
| TLS API | `:1443` (`TLS_ADDR`) | The same as the API port, when `TLS_CERT_PATH` is set. |

**The metrics and health endpoints are on the debug port, not the API port**,
so a probe or a scrape aimed at `:1080` gets the elastic proxy's "no such
route" instead.

RPC paths, both served from the same implementation:

* `POST /twirp/elephant.index.<Service>/<Method>`
* `POST /elephant.index.<Service>/<Method>` — Connect, plus gRPC and gRPC-Web
  in-cluster only.

Nothing in front of the service routes on the `/twirp/` prefix, so the Connect
paths needed no ingress change.

### Health checks

| Check | Required? | What it does |
|---|---|---|
| `postgres` | **required** | `ListIndexSets`. Failure deregisters the replica. |
| `opensearch` | optional | Lists `documents-*` on the active cluster with a 500ms timeout. |

The OpenSearch check is deliberately optional: it reports `"ok": false` in the
`/health/ready` body and drives `health_check_up{name="opensearch"}` to 0, but
it does not take the replica out of rotation. **Nothing reacts to it unless
somebody alerts on that gauge.** An OpenSearch outage would otherwise
deregister every replica at once, turning a degraded dependency into a total
outage.

The check returns success when there is no active index set at all
(`pgx.ErrNoRows`), so a fresh deployment is ready before it has anything to
serve.

## Data flows

### 1. Indexing

```
repository eventlog
      |
      | koonkie follower, StartAfter = index_set.position, 10s poll when caught up
      v
  Indexer (job lock: indexer-<set name>)
      |
      | load document + metadata from the repository (authenticated client)
      v
  BuildDocument -> flat property structure
      |
      | revisor schemas + document data -> mapping
      v
  index worker per (type, language)
      |
      | PUT mapping if new fields appeared, then _bulk
      v
  documents-<set>-<type>-<language>   in the set's cluster
      |
      | only after the batch is written:
      v
  UPDATE index_set.position
```

The load-bearing detail is the last step. **The position is written after the
batch, so a crash replays rather than skips**, and replay is safe because
documents are written under their own UUID.

The exception is a batch whose items *partly* failed: the position still
advances, so those individual documents are lost until something else replays
them. `elephant_indexer_doc_total{result=~".*_err"}` is the only thing that
will tell you, which is why it is on the watch list.

### 2. Search

```
client --> :1080 --> auth middleware (OIDC) --> SearchV1 handler
                                                    |
                                          active index set + client
                                                    v
                                        documents-<active set>-*
```

No repository involvement, which is why search survives a repository outage.
`GetFlatDocument` is the exception: it can flatten a document straight from
the repository, so it fails when the repository is down even though the rest
of search does not.

### 3. Percolation

```
Indexer (active set only) --> Coordinator --> percolator_event_payload  (UNLOGGED)
                                   |          + NOTIFY percolate_event
                                   |          in ONE transaction
                                   v
                            Percolator (job lock: percolator)
                                   |
                                   | percolate from last id to head
                                   v
                            percolator_event (UNLOGGED) + NOTIFY percolated
                                   |
                                   v
                            in-flight PollSubscription responds
```

The operational weight here is what the word UNLOGGED means: **those two
tables do not survive a database restart or failover, and that is by design.**
Indexing is prioritised over delivery. A failover drops in-flight percolation
state, subscribers miss those events, and nothing retries. Clients that need
completeness are expected to reconcile against the event log themselves.

The second thing worth knowing: percolation is serial and unbatched, so it is
the first thing to fall behind under load, and it falls behind silently — the
indexer does not wait for it.

## Single-leader work

| Lock | Does | When nobody holds it |
|---|---|---|
| `indexer-<set name>` | Follows the log and indexes into that set | That set stops advancing. Search still serves it. |
| `percolator` | Percolates events and delivers to subscribers | Subscriptions stop delivering. Indexing is unaffected. |

Every replica runs every goroutine and competes for these locks, so a replica
holding no lock is normal and healthy. The pruning of percolation state is
*not* lock-gated — every replica prunes, which is harmless because the deletes
are by age.

## Where state lives

| Store | Holds | Authoritative? |
|---|---|---|
| Postgres `index_set`, `document_index` | Index sets, their position and their indices | Yes, for what exists and where it is. |
| Postgres `cluster` | OpenSearch clusters and their credentials (AES-256-GCM) | Yes. |
| Postgres `subscription`, `percolator`, `percolator_document_index` | Subscriptions and their percolator queries | Yes. |
| Postgres `percolator_event`, `percolator_event_payload` | In-flight percolation | **No — UNLOGGED, lost on restart.** |
| Postgres `indexing_override` | Per-type field mapping overrides | Yes. |
| Postgres `app_state` | Percolator position | Yes. |
| Postgres `job_lock` | Leader election | Yes. Vendored from elephantine. |
| OpenSearch | The indexed documents | No — derived, rebuildable by re-indexing. |

**Everything in OpenSearch is derived and can be rebuilt from the repository
event log.** Nothing in Postgres can, apart from the two unlogged tables.

## Bootstrap order

1. **Postgres reachable and migrated.** Migrations are never run by the
   service; `mage sql:migrate` locally, `setup db migrate` in
   elephant-platform. Starting against an unmigrated database fails on the
   first query.
2. **The password key is set and 32 bytes.** Startup fails on a malformed key
   before anything else happens. A key that is *different* from the one the
   stored cluster passwords were encrypted with does not fail at startup — it
   fails when a client is built for a cluster.
3. **The repository is reachable.** The schema loader fetches revisor schemas
   at startup and startup fails without them.
4. **At least one cluster is registered and one index set exists.** Neither is
   created for you — see the note in
   [What is not in place yet](#what-is-not-in-place-yet). Until then the
   service is ready and serves nothing.
5. **An index set is active.** Search against no active set fails; the
   readiness check does not.

Out of order, the failures are all at startup except step 4, which is the one
that leaves a healthy-looking replica serving nothing.

## Recovering the indexed half

OpenSearch content is derived, so recovery is a re-index. It is the same
operation as a routine re-index and is described in the
[README](../README.md#re-indexing); the phases are:

1. `Reindex` — creates a new index set, optionally naming a different cluster.
   Indexing into it starts immediately.
2. Watch it catch up: `eventlog_follower_position{follower="<new set>"}`
   against the active set's, or `ListIndexSets`.
3. `SetIndexSetStatus` with `active: true` — a read-path switch, and
   reversible. It refuses a set lagging by more than 10 events unless
   `force_active` is set.
4. Disable the old set, then `DeleteIndexSet`.

**Do not delete the old set until the new one has been verified**, because
deletion is the only irreversible step.

## Failure modes

### Search results are quietly incomplete

The index is missing documents nobody reported. Either a batch partly failed
and the position moved past it, or a mapping change was dropped so a field is
absent and queries on it match nothing.

*Signal:* `elephant_indexer_doc_total{result=~".*_err"}` non-zero, or
`elephant_indexer_ignored_mapping_total` non-zero for the field being queried.

*Action:* for failed documents, re-index into a new set. For a dropped
mapping, a re-index is the only fix — the mapping cannot be widened in place,
which is why the counter matters more than it looks.

### Indexing has stopped advancing

*Signal:* `eventlog_follower_position` flat for the active set. Then split on
`elephant_indexer_failures_total`: climbing means it is retrying against
something that keeps refusing; flat means no replica is running that indexer.

*Action:* if it is retrying, find what it is retrying against — the repository
or the cluster. If nothing is running it, check `pg_job_lock_*` and
`task_restarts_total`: a wedged holder keeps the lock until it stops touching
it.

### Subscriptions stop delivering, indexing is fine

*Signal:* `elephant_indexer_percolator_position` flat while the active
follower advances.

*Action:* check who holds the `percolator` lock. Note the gauge is only true
on that replica, so aggregate with `max` before concluding it is flat. If
percolation is merely behind rather than stopped, that is the known serial
bottleneck — see [Pending work](architecture.md#pending-work).

### Subscriptions look healthy but deliver nothing

A subscription registered whose query never became a percolator document.

*Signal:* `elephant_indexer_percolator_lifecycle_total{event="query-doc-error"}`
climbing.

*Action:* the client sees a valid subscription and an empty stream, so nothing
will be reported from that side. Read the percolator's logs for the query that
failed.

### Percolation events were dropped

*Signal:* `elephant_indexer_percolation_total{event="queue_failed"}` non-zero,
or a Postgres restart or failover in the timeline.

*Action:* nothing to recover — the tables are unlogged and there is no retry.
Tell affected clients to reconcile against the event log. This is designed
behaviour, not a defect.

### Everything is refused with 401

*Signal:* `rpc_protocol_responses_total{code="unauthenticated"}` across all
methods.

*Action:* the OIDC provider or the JWKS fetch. **Note that an invalid token is
answered `401` where it used to be `403`** — a dashboard or ingress rule still
keyed on 403 will look like the problem moved rather than that the status
changed.

### A caller gets a dropped connection with no error

*Signal:* a panic with `unimplemented` in the logs.

*Action:* they called `SearchV1.EndSubscription` or
`Management.PartialReindex`, which are mounted but unimplemented. Nothing to
fix at runtime; tell the caller not to. `EndSubscription` needs no scope, so
any caller with a valid token can trigger it.

### The cluster lost documents after an upgrade

*Signal:* documents and whole indices missing after an in-place OpenSearch
version upgrade. Observed going from v2.5 to v2.19 in stage.

*Action:* re-index into a new set. **Prevention is the real answer: never
upgrade a cluster under a live index set** — stand up a new cluster on the new
version and re-index into it. See
[architecture.md](architecture.md#upgrading-opensearch-what-not-to-do-again).

## What to watch, in order

1. **`elephant_indexer_doc_total{result=~".*_err"}`** — the only signal for
   silently missing documents, and the position moves past them regardless.
2. **`eventlog_follower_position` for the active set** — flat means the write
   half has stopped; everything else about indexing follows from it.
3. **`elephant_indexer_ignored_mapping_total`** — each increment is a field
   that is permanently unqueryable in that index until a re-index.
4. **`elephant_indexer_percolator_position` versus the active follower** — the
   subscription delivery backlog, and the first thing to lag under load.
5. **`health_check_up{name="opensearch"}`** — the optional check nothing else
   reacts to, so it is invisible unless it is on this list.

## Common operations

**Start a re-index.** `Reindex` with an optional `cluster`; see
[README](../README.md#re-indexing).

**Check catch-up progress.** `ListIndexSets`, or compare
`eventlog_follower_position` between followers.

**Activate an index set.** `SetIndexSetStatus` with `active: true`. On a
`failed_precondition` refusal, either wait for it to catch up or pass
`force_active`. **That code is `412` on Twirp and `400` on Connect** — read
the code in the body, not the status.

**Pause indexing into a set.** `SetIndexSetStatus` with `enabled: false`.

**Register a cluster.** `RegisterCluster`. A password is encrypted with the
service's password key, so a cluster registered under one key is unusable
after a key change.

**Run migrations.** `go run ./cmd/setup db migrate` in elephant-platform, or
`mage sql:migrate` locally. Never at startup.

## Security

**Inbound.** Every RPC path and the proxy sit behind the same authentication
middleware. `index_admin` for all of `Management`; `search` or `index_admin`
for `SearchV1`; `search` for the proxy's `_search`, and `doc_admin` for
cross-index access through it. An unidentifiable token is `401`, an identified
caller lacking a scope is `403`.

**Outbound.** The service holds a client credentials token with
`eventlog_read`, `doc_read_all` and `schema_read` and uses it to follow the log
and load documents and schemas. That is background work on its own behalf, not
work for a caller, which is what makes the service account appropriate there.

`GetFlatDocument` is the one method that reads from the repository while
serving a caller, and it is careful about it: it goes through the
**anonymous** repository client and forwards the caller's own bearer token on
the request, so the repository applies the caller's read permissions. **A
caller cannot use it to reach a document their own token would be refused** —
which is the whole point, because the service's own token carries
`doc_read_all`. The reasoning is recorded in a comment at the call site.

That forwarding currently uses `twirp.WithHTTPRequestHeaders`. It still works,
because the repository client is still a Twirp client; when that client moves
to Connect it becomes `rpc.WithOutgoingHeaders`, and **the check that it still
forwards is that `GetFlatDocument` keeps working against a caller whose token
is narrower than the service's** — not that it compiles.

**Keys.** The 32-byte `--password-key` encrypts cluster passwords with
AES-256-GCM in a `v1.<base64>` envelope. It is not rotatable without
re-registering every cluster: nothing re-encrypts stored passwords under a new
key.

**Not a write path.** This service never writes to the repository. Everything
it holds is either derived from the log or its own configuration, so the worst
a compromised replica does to document state is nothing — the exposure is read
access across all documents, via `doc_read_all`, and the cluster credentials.

## What is not in place yet

**A default index set is not created, despite the flag that says it should
be.** `--opensearch-endpoint` is parsed, and credentials in its userinfo are
extracted correctly, but the parsed URL is assigned to a shadowed variable
inside the `if` block in `cmd/index/main.go` and never reaches
`RunIndex`. `DefaultCluster` is therefore always nil, and
`EnsureDefaultIndexSet` never runs. **A fresh deployment has to register a
cluster and create an index set through the management API**, and the flag
being set is not evidence that it did not need to. The test suite passes
`DefaultCluster` directly and so does not cover this path.

**`elephant_indexer_mapping_update_total` is registered but never
incremented.** It is permanently zero; do not build a panel on it.

**Two RPC methods panic.** `SearchV1.EndSubscription` and
`Management.PartialReindex` are mounted and reachable but
`panic("unimplemented")`. `EndSubscription` panics before checking a scope.

**The elastic proxy is marked for retirement** in the code but still mounted
on `/` and still the interface some clients use.

**Percolation has no delivery guarantees and is not meant to.** See
[architecture.md](architecture.md#delivery-guarantees-there-are-none) — the
unlogged tables, the prioritisation of indexing, and the missed events at a
re-index cutover are all deliberate.
