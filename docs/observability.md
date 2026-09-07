# Observability

Every metric Elephant index exports, and what a change in each one means. The
definition is in the name; what a dashboard needs is the direction that is bad,
what is routinely non-zero, and whether a number is lag or loss.

| Document | What it settles |
|---|---|
| [../README.md](../README.md) | Orientation and the working reference: layout, build, how to run it, every configuration flag. |
| [architecture.md](architecture.md) | How the service is built: process model, data flow, subsystems, API surface. |
| [ops.md](ops.md) | The operator's view: dependencies, ports, bootstrap order, failure modes and their signals. |
| **observability.md** (this document) | Every metric and what a change in it means. |

This document does not say what to do about a number — that is
[ops.md](ops.md), whose failure modes name these metrics as their signals.

## Where the metrics come from

The service's own collectors are declared in one place, `index/metrics.go`,
and registered through the registerer passed to `NewMetrics`. Only `main`
picks `prometheus.DefaultRegisterer`; the test suite builds the whole set
against a fresh registry, which is what keeps registration order from
mattering.

Three sets come from libraries rather than from this repository, and are
documented by them:

| Set | From | Covers |
|---|---|---|
| `rpc_*` | `elephantine` | Request, duration, response and per-protocol response counters for both RPC stacks. |
| `eventlog_follower_position` | `koonkie` | The event log position per follower. |
| `pg_job_lock_*`, `pgxpool_*`, `task_restarts_total`, `client_*` | `elephantine` | Job lock state, connection pool, task supervision, outbound HTTP clients. |

The service's own metrics are prefixed `elephant_indexer_`. That does not
match the fleet convention of a short service-name prefix, but they are
production metrics with dashboards and alerts on them, so they are left
alone.

## Indexing

**The pair to watch is `eventlog_follower_position` against
`elephant_indexer_failures_total`.** A position that stops advancing while
failures climb is a stuck indexer that is retrying; a position that stops
advancing with no failures is an indexer that is not running at all, which is
usually a job lock held by a replica that has wedged.

* `eventlog_follower_position{follower,state}` — the last event log position
  the follower reached. `follower` is the index set name, so during a re-index
  there are two series and the gap between them is the catch-up backlog.
  `state` is `tail` when caught up and `compact` when reading the compacted
  log, which is how a fresh or badly lagging set reads. **A set stuck in
  `compact` for a long time is still catching up, not broken.**
* `elephant_indexer_failures_total{name}` — batch failures per index set. Each
  increment is one loop iteration that rewound and will retry in five seconds,
  so this is lag rather than loss. A steady rate means something upstream is
  refusing consistently; an isolated increment is a blip that already
  recovered.
* `elephant_indexer_doc_total{type,index,result}` — per-document outcomes from
  the bulk API, emitted per item so a partial batch success is visible.
  The `result` label is the bulk operation, `index` or `delete`, with an
  `_err` suffix when it failed — so `index`, `delete`, `index_err` and
  `delete_err`. A `delete` that OpenSearch answered `404` counts as a success,
  since the document being absent is the outcome that was asked for.
  **Non-zero failures here are documents that are not in the index**, which is
  loss until the event is replayed, and it will not be replayed on its own
  because the position advances past a batch whose items partly failed.
* `elephant_indexer_unknown_events_total{type}` — event types from the log the
  indexer does not handle. Routinely non-zero and harmless when the repository
  adds an event type this service does not care about; a new label value
  appearing right after a repository deploy is worth reading as "check whether
  we should be handling this".
* `elephant_indexer_enrich_errors_total{type,index}` — failures loading or
  enriching a document before it could be flattened. Read it against
  `elephant_indexer_doc_total`: enrichment failing means the document never
  reached the bulk request at all.

## Mappings

* `elephant_indexer_ignored_mapping_total{index,property}` — mapping changes
  that were dropped because OpenSearch would have rejected them as a conflict.
  **Every increment is a field that will not be queryable in that index**, and
  the field is silently absent rather than wrong, so nothing else will report
  it. A new `property` label value is the signal that a schema change has
  landed that this index cannot accommodate; fixing it means a re-index into a
  new set.
* `elephant_indexer_mapping_update_total{index}` — **registered but never
  incremented.** It is always zero, so do not build a panel on it and do not
  read a flat line as "no mapping updates". Mapping updates happen; they are
  simply not counted.

## Percolation

**The pair to watch is `elephant_indexer_percolator_position` against the
active indexer's `eventlog_follower_position`.** The gap is how far behind
subscription delivery is; percolation is serial and unbatched, so it is the
first thing to lag under load.

* `elephant_indexer_percolator_position` — the last event id the percolator
  processed. A gauge, and only meaningful on the replica holding the
  `percolator` lock. **On every other replica it reports whatever it last saw,
  or zero if it never held the lock**, so aggregate it with a max across
  replicas rather than an average.
* `elephant_indexer_percolation_total{event,location}` — percolation requests
  and their disposition. The `event` values that matter:
  * `requested` (`location` is the index set name) — an indexer asked for
    percolation after indexing a batch.
  * `queued` — the coordinator accepted it and wrote the payload.
  * `inactive_set` — dropped because the requesting set is not the active one.
    **Routinely non-zero for the whole duration of a re-index, and means
    nothing on its own.** Non-zero when no re-index is running means the active
    set is not what you think it is.
  * `queue_failed` — the payload transaction failed, so those events will
    never be percolated. This is dropped delivery, and it is the one value
    here that is loss.
  * `percolate-event` (`location` is `percolator`) — an event was percolated.
* `elephant_indexer_percolator_lifecycle_total{event}` — the percolator's own
  loop. `acquire-lock`, `start` and `stop` trace the lock changing hands;
  `triggered` counts wakeups from a notification and `poll` counts timer-driven
  passes; `no-work` is the idle case and is routinely the largest.
  `end-iteration` closes each pass. `query-doc-preseed`, `query-doc` and
  `query-doc-error` cover creating percolator documents for a subscription's
  query — **`query-doc-error` climbing means subscriptions are registered whose
  queries never became percolator documents, so those subscriptions deliver
  nothing** while looking healthy to the client.

A `start` without a matching `stop`, repeatedly, is the percolator restarting
under supervision; read `task_restarts_total` alongside it.

## RPC

Defined by `elephantine`; the two facts specific to this service:

* `rpc_protocol_responses_total{service,method,protocol,code,client_id}` is
  the migration instrument. **`protocol="twirp"` falling to zero for a method
  is what says its Twirp mount can be retired**, and `client_id` names the
  applications that still have to move.
* `failed_precondition` no longer has a status of its own. An index set lag
  refusal is `412` on Twirp and `400` on Connect, so a panel counting `412`
  undercounts as callers move. Read `code` on
  `rpc_protocol_responses_total` instead of the status.

Two gaps to know before trusting a Connect panel: a gRPC or gRPC-Web response
reports `status` `200` whatever the outcome, because those protocols carry the
code in trailers; and framework-level failures on the Connect stack — a
malformed body, an unknown method, a body over the limit — are answered before
any interceptor runs and so are not counted at all, where Twirp reported them
as `malformed` and `bad_route`.

## State gauges and conventions

Two conventions apply to everything above.

**A gauge is only true on the replica doing the work.** The percolator
position and the follower positions are set by whichever replica holds the
relevant job lock. Every other replica reports a stale value or zero, and
neither is an error. Aggregate with `max`, and treat a single replica's flat
gauge as no evidence at all.

**A counter that is routinely non-zero is not an alert.**
`elephant_indexer_unknown_events_total`, `percolation_total{event="inactive_set"}`
and `percolator_lifecycle_total{event="no-work"}` all climb in normal
operation. The ones that mean something is wrong every time they move are
`elephant_indexer_doc_total{result=~".*_err"}`,
`percolation_total{event="queue_failed"}`, and
`elephant_indexer_ignored_mapping_total`.
