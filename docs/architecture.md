# Architecture

How Elephant index is built: the process model, the path a document takes from
the repository event log into OpenSearch, the subsystems with their own
lifecycles, and the API surface. Start here to understand the system before
changing it.

| Document | What it settles |
|---|---|
| [../README.md](../README.md) | Orientation and the working reference: layout, build, how to run it, every configuration flag. |
| **architecture.md** (this document) | How the service is built: process model, data flow, subsystems, API surface. |
| [ops.md](ops.md) | The operator's view: dependencies, ports, bootstrap order, failure modes and their signals. |
| [observability.md](observability.md) | Every metric the service exports and what a change in it means. |

This document does not cover how to run or recover the service; that is
[ops.md](ops.md). It names metrics where they are the evidence for a design
decision, but [observability.md](observability.md) defines them.

## Process model

One process serves the API and runs the indexing. Everything below is started
by `RunIndex` in `index/server.go`, under an `elephantine.ErrGroup` with a
10-second graceful shutdown.

| Goroutine | Under a lock? | Conditional on |
|---|---|---|
| `server` — HTTP listener, both RPC stacks and the elastic proxy | no | always |
| `coordinator` — index set lifecycle and the notification subscriber | no | always |
| One `Indexer` per enabled index set | `indexer-<set name>` | `--no-indexer` unset |
| One index worker per type × language, inside each indexer | no (owned by its indexer) | documents of that type arriving |
| `percolator` | `percolator` | always, created by the coordinator |
| `Percolator.cleanup` — prunes percolation state and unused percolators, every minute | **no** | always |
| `Coordinator.cleanupLoop` — retries deleting the indices of deleted index sets, every 12–24h | no | always |

**Every replica runs every goroutine; the job locks are what make the work
single-leader.** A replica that holds no lock still serves search, still
proxies, and still answers subscription polls — it is only the indexing and
percolation that wait. That is the property that makes replicas useful at all,
and it is why the readiness check must not fail on a lock it does not hold.

`--no-indexer` is the one switch that changes the shape rather than the
scale: the coordinator then sets up only an OpenSearch client for the active
set and starts no indexers, so the process is a read-only search frontend.

## Data flow

### 1. Following the event log

Each `Indexer` (`index/index.go`) owns one index set and follows the
repository event log through `koonkie.NewLogFollower`, starting after the
position stored in `index_set.position` and polling with a 10-second wait when
caught up.

**The position is written after the batch it describes has been indexed, not
before.** A crash mid-batch therefore replays the batch rather than skipping
it, and indexing is idempotent because a document is written under its own
UUID. On failure the loop rewinds the follower to the position it started the
iteration at, waits five seconds, and retries — so a transient repository or
OpenSearch outage is lag, never loss, and `elephant_indexer_failures_total`
climbing with the position gauge flat is what that looks like from outside.

### 2. Enrichment and flattening

An event names a document; the indexer loads the document and its metadata
from the repository and hands it to `BuildDocument` (`index/build.go`), which
flattens it into a flat property structure — one map from field path to
values.

Field names are derived from block attributes, and the rules are worth stating
because they decide what a client can query:

* Meta and content blocks key on `type`: `document.meta.core_definition.data.text`.
* Link blocks key on `rel`: `document.rel.evidence.url`.
* The primary attribute is not indexed separately, since it is already encoded
  in the field name.
* When the primary attribute is missing, the fallback order is `type`, `rel`,
  `role`, `name`, and the value is prefixed with the attribute it came from —
  a link with no `rel` but a `type` of `text/html` becomes
  `rel.type__text_html`. With nothing to fall back on the key is `__unknown`.
* Every non-alphanumeric character except a space becomes an underscore.

### 3. Mappings

The revisor schemas loaded from the repository are combined with the actual
document data to construct the OpenSearch mapping
(`index/mappings.go`). **A property is never indexed before a mapping exists
for it**, so the worker updates the index mapping as new fields appear and
only then writes documents.

A mapping change that OpenSearch would reject as a conflict is dropped rather
than retried, and counted on
`elephant_indexer_ignored_mapping_total{index,property}`. A non-zero rate
there means documents are being indexed without a field a client may be
querying — the field is silently absent, not wrong, which is the harder
failure to notice.

### 4. Index workers

Within an indexer, one worker per document type and language
(`index/index_worker.go`) batches documents and writes them with the bulk
API. Bulk responses are interpreted per item, so a batch is a partial success:
per-item outcomes land on
`elephant_indexer_doc_total{type,index,result}` **before** a retryable error
is acted on, so a partial success stays observable rather than being hidden by
the retry.

Indices are named `documents-<set name>-<type>-<language>`, so index set
"factual-tiger", type `core/article` and language `sv-se` give
`documents-factual-tiger-core_article-sv-se`. A language code with no region
gets `-unspecified`: `sv` gives `documents-factual-tiger-core_article-sv-unspecified`.

**A separate index per type and language is what lets mappings differ without
conflicting**, and what lets each index use a language-specific ICU analyzer
(`index/language-settings.go`) instead of one analyzer for every language.

## Index sets and re-indexing

An index set is a named collection of indices — one per type × language —
carrying its own position in the event log. Several can exist; exactly one is
active and serves search traffic. Names are random codenames
(`lucasepe/codename`), so "factual-tiger" identifies a set without encoding
anything about it.

Re-indexing is blue-green: create a new set, optionally in another cluster,
let it catch up, then activate it. **Activation is a read-path switch only, so
it is reversible** — the old set keeps its documents and can be made active
again as long as it has not been deleted.

`SetIndexSetStatus` refuses to activate a set that lags the active one by more
than 10 events unless `force_active` is set. The lag check is the guard
against activating a set that would serve stale results; the override exists
because a deliberate cutover during a backlog is sometimes the right call.

### Upgrading OpenSearch: what not to do again

A blue/green upgrade of a cluster in place, from v2.5 to v2.19 in stage, lost
documents and indices. The cause was never established, but on-demand index
creation is the likely culprit. **Do not upgrade a cluster under a live index
set.** Create a new cluster on the new version, register it, re-index into a
new set in that cluster, and switch when it has caught up. That is also
reversible, which an in-place upgrade is not.

## Percolation and subscriptions

A search can register a subscription, which stores the query as a percolator
in the database. Documents are matched against those queries after indexing,
and clients long-poll for the results.

### Why percolator documents are created lazily

There is one percolator index per document type *and language*, and the set of
languages is not known ahead of time. Creating a percolator document eagerly
would mean guessing the languages; instead the document is created the first
time a document of that type and language is indexed, and a new language index
appears while a subscription is already running.

### Flow

```
Indexer                    Coordinator                   Percolator                Client
  |                            |                              |                       |
  | indexed a batch            |                              |                       |
  |--- percolate request ----->|                              |                       |
  |                            | drops it unless this is      |                       |
  |                            | the ACTIVE set               |                       |
  |                            |   (metric: inactive_set)     |                       |
  |                            |                              |                       |
  |                            | store event id + payload     |                       |
  |                            | (computed fields + newsdoc)  |                       |
  |                            | in percolator_event_payload  |                       |
  |                            | + cache in process           |                       |
  |                            |                              |                       |
  |                            |-- NOTIFY percolate_event --->|                       |
  |                            |   (one tx: payload + notify) | percolate from last   |
  |                            |                              | percolated id to head |
  |                            |                              |                       |
  |                            |                              | store results,        |
  |                            |                              | NOTIFY percolated     |
  |                            |                              |---------------------->|
  |                            |                              |   in-flight poll wakes
```

**The payload and the notification are written in one transaction**, so a
notification never announces a payload that is not there. The in-process cache
in front of `percolator_event_payload` exists to keep the percolator from
reading back what the coordinator just wrote.

Only the active indexer's percolation requests are honoured. A request from a
catching-up set is dropped and counted on
`elephant_indexer_percolation_total{event="inactive_set"}`, which is expected
to be non-zero for the whole duration of a re-index and means nothing on its
own.

### Delivery guarantees: there are none

This is a design decision, not a gap, and clients have to be told:

* `percolator_event` and `percolator_event_payload` are **unlogged** tables.
  They are faster and they do not survive a database restart or failover.
* Percolation failure never halts the indexer. **Indexing documents is
  prioritised over delivering percolation results.**
* No care is taken to avoid missing events when the active index set changes,
  so a re-index cutover can drop notifications.

Subscriptions are for keeping a UI approximately current. A client that needs
completeness compares against the event log itself.

### Long-poll deadlines

`PollSubscription` waits up to `max_wait_ms` (default 10s) for events, in a
loop of at most two iterations with a batch window of `batch_delay_ms`
(default 200ms) once the first event arrives.

**Three things can end the wait, and they are not the same answer.** Reaching
`max_wait_ms` with nothing to report is a successful empty response — the
ordinary idle case. A request context that ends the wait is either the
caller's deadline or the caller going away, and is answered
`deadline_exceeded` or `canceled` respectively.

The distinction is load-bearing on the Connect stack, because Connect turns a
`Connect-Timeout-Ms` header into the handler's context deadline and enforces
it, where Twirp ignores client deadlines entirely. It also cannot be left to
the framework: connect-go checks the request context *before* it calls a
handler and not after, so a successful empty response returned past the
deadline is not corrected on the way out — it would be recorded as a `200` for
a call that timed out.

### Retention

The percolator's own cleanup goroutine runs every minute and expires by age:
percolation events after 60 minutes, event payloads after 90 minutes, and a
subscription 30 minutes after it was last touched — polling a subscription
touches it, so a client that stops polling loses it. A percolator is removed
once no subscription references it, in two phases: marked for deletion, then
its OpenSearch percolator documents purged.

**That loop is not gated on the `percolator` job lock**, so every replica
prunes. The deletes are by age and idempotent, so concurrent runs are
harmless, but it means the pruning keeps happening on a replica that is
percolating nothing.

### Pending work

Recorded here because each one is a known cost, not a wish:

* **Language-neutral percolator indices per type**, with percolator documents
  created ahead of time for subscriptions marked language-neutral. Less stored
  percolator state, and it moves document creation out of the percolation loop,
  which lowers latency. A variant achieves the same for subscriptions pinned to
  one language.
* **Shed percolation under high throughput.** A migration touching millions of
  documents makes percolation work that nobody is waiting for.
* **A counting bloom filter** (for example `tylertreat/BoomFilters`) to decide
  whether to emit a not-matched event. Today not-matched is always emitted; a
  probabilistic "probably not" would be cheaper.
* **Percolation concurrency.** Percolation is serial and unbatched, so it lags
  first under load. Batching documents into each percolation call is the low
  hanging fruit; running different types concurrently is the next step.

## Cluster registry and credentials

Clusters are rows in `cluster`, each with its authentication as JSON. An index
set names the cluster it lives in, which is what allows a re-index to target a
different cluster.

Passwords are encrypted with AES-256-GCM under the 32-byte key from
`--password-key` and stored in a versioned envelope, `v1.<base64>`
(`index/aes.go`). The key is needed to register a cluster with password
authentication, to parse credentials out of the default OpenSearch URL, and to
build a client for a stored cluster. `--managed-opensearch` selects AWS IAM
request signing instead, and a cluster may also carry `insecure_tls` or a
`ca_cert`.

## API surface

Two RPC services, each served on two path families from the same
implementation (`registerAPIs` in `index/server.go`):

| | Twirp | Connect |
|---|---|---|
| Path | `POST /twirp/elephant.index.<Service>/<Method>` | `POST /elephant.index.<Service>/<Method>` |
| Protocols | Twirp JSON and protobuf | Connect, plus gRPC and gRPC-Web in-cluster only |
| Error body | `{"code","msg","meta"}` | `{"code","message","details"}` with an `elephantine.rpc.ErrorMeta` detail |
| JSON field names | as the `.proto` declares them, `index_sets` | protojson lowerCamelCase, `indexSets` |

Both mounts are configured from one `elephantine.ServiceOptions`, so
authentication, logging and metrics are the same on both by construction
rather than by two chains kept in step. Requests accept either JSON field
spelling on both stacks.

Three codes differ in HTTP status: `failed_precondition` is `400` on Connect
against Twirp's `412`, `canceled` `499` against `408`, and `deadline_exceeded`
`504` against `408`. **Read the code from the body, not the status** —
`failed_precondition` is what an index set lag refusal returns, so this is a
live difference and not a theoretical one.

**Handlers return `*connect.Error` through the `elephantine/rpc` helpers, and
no handler constructs a Twirp error.** A Twirp caller is answered by
translating on the way out, through the interceptor `opts.ServerOptions()`
installs, so the Twirp mount needs no knowledge of it — that is why the file
that mounts Twirp does not import `twitchtv/twirp` at all.

Two rules keep that working, and both are easy to undo by accident:

* **Never wrap an RPC error.** Wrapping it in another coded error replaces the
  code the handler picked, and wrapping it in a bare `fmt.Errorf` leaves a
  prefix that is written and never read, because the caller is answered from
  the innermost coded error. Return the helper's result as it is and put the
  context in the message you pass the helper. `internal.NewSearchRequest` is
  the case to know: it validates caller input and returns coded errors, so
  `Query` and `MultiSearch` return its error untouched.
* **Every handler error carries a code.** The two stacks default an uncoded
  error differently — Twirp `internal`, Connect `unknown` — so a bare
  `fmt.Errorf` escaping a handler changes meaning between them. Helpers may
  return plain errors as long as the handler codes them.

`RequireAnyScope` in `index/permissions.go` is the service's own copy rather
than `rpc.RequireAnyScope`. It returns `rpc` errors, but its message and its
lack of a `required_any_of_scopes` meta key are the ones callers have always
seen, and adopting the shared helper would change both.

Parity between the stacks is tested rather than assumed: `TestErrorParity`
performs the same failing call on both and asserts the code, the message and
the metadata match, and the wire golden files pin two error bodies per stack.

### Scopes

| Method | Scopes accepted |
|---|---|
| All of `Management` | `index_admin` |
| `SearchV1.Query`, `MultiSearch`, `GetMappings`, `GetFlatDocument`, `PollSubscription` | `search` or `index_admin` |
| Elastic proxy `_search` | `search` |
| Elastic proxy, cross-index access | `doc_admin` |

An invalid token is answered `unauthenticated` (401) by the authentication
middleware before the request reaches a handler; `permission_denied` (403)
means an identified caller that lacks a scope.

### Unimplemented methods

Two methods are mounted and reachable but `panic("unimplemented")`:

* **`SearchV1.EndSubscription`** — panics before any scope check, so any
  caller with a valid token reaches it.
* **`Management.PartialReindex`** — checks `index_admin` first, so only an
  administrator reaches it.

`net/http` recovers the panic and drops the connection, so the process
survives, but the caller gets no usable error and the log gets a stack trace.
Subscriptions are instead reaped by the percolator's cleanup loop once they
stop being touched.

### The elastic proxy

A pass-through `_search` endpoint against the active index set, mounted on `/`
as a catch-all (`index/proxy.go`). It is marked for retirement in the code and
exists for clients that predate the search API.

**The Connect subtrees are more specific mux patterns than `/`**, so mounting
Connect does not shadow the proxy and the proxy does not shadow Connect. Any
future mount at a bare prefix has to be checked against this.

## Where the schemas come from

**The repository is called with Connect clients** (`repositoryconnect`), so it
has to be a version that serves them; see
[ops.md](ops.md#bootstrap-order). The service's own background work — following
the log, loading documents and schemas — carries the service token in the
`http.Client`, while the handlers that serve a caller forward the caller's
token per request with `rpc.WithOutgoingHeaders`, which reaches the wire only
because the anonymous client is built with `rpc.PropagateHeaders()`.

`index/schemas.go` loads revisor schemas from the repository at startup and
keeps them current. They are the input to mapping construction, so **a schema
the loader could not fetch is an index whose mapping cannot be extended** —
the indexer keeps running against the mappings it already has.
