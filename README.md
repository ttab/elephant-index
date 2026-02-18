# Elephant index

Elephant index follows the event log from the elephant repository and indexes the updated documents in OpenSearch.

Before indexing, documents are flattened to a [flat property structure](index/testdata/raw_1.values.json) and the [revisor](https://github.com/ttab/revisor) schemas of the repository are used together with the document data to construct the [mappings for the open search index](index/testdata/raw_1.mappings.json).

The indexer will add to the index mappings as needed, and no new properties will be indexed without first creating a mapping for it. A separate index is created for each document type and language. This lets us avoid conflicts between mappings of separate document types, and allows us to use language specific analyzers for better multilingual support.

## Password encryption key

The service requires a 32-byte hex-encoded encryption key (`--password-key` flag or `PASSWORD_ENCRYPTION_KEY` environment variable) used to encrypt OpenSearch cluster passwords before storing them in the database. Passwords are encrypted with AES-256-GCM and stored in a versioned format (`v1.<base64>`).

The key is used when:

- Registering a new cluster with password authentication via the management API.
- Parsing credentials from the default `--opensearch-addr` URL.
- Creating OpenSearch clients (decrypting stored passwords).

> **Tip:** Generate a key with `head -c 32 /dev/urandom | xxd -ps -cols 0`

## Upgrading OpenSearch

We observed a loss of documents/indexes in the cluster when doing a blue/green upgrade from v2.5 to v2.19 in our stage environment. The exact cause is not known, but it's likely to be caused by the on-demand creation of indexes. Best practice for now is to always create a new cluster with the new version, re-index into that cluster, and then switch over when the new index set has caught up. That has the added benefit of being a reversible operation, so that we can switch back to the old cluster if we experience issues.

## Index names

The elephant index creates indexes in the pattern `documents-[random name]-[type]-[language]`. So if the name of the index set is "factual-tiger", the type of the document is "core/article", and the language is "sv-se", the following index will be used: "documents-factual-tiger-core_article-sv-se". For language codes without region the suffix "-unspecified" will be used, e.g. "sv" -> "documents-factual-tiger-core_article-sv-unspecified".

## Subscriptions and percolation

When searching, clients can create a subscription for the query. This will register the percolation query and subscription in the database. The next time a document with a type matching the query is indexed a percolator document will be created for the language of the document.

The reason that the percolator document isn't created immediately is that there are multiple percolator indices per document type (one for each language) and the number of languages are not known ahead of time. If a document with a new language is indexed while the query is running a new language index is created while the subscription is running.

### Delivery guarantees

We do not have delivery guarantees when it comes to subscriptions. The percolation event tables are unlogged (non-persistent, faster) and are lost on database restart/failover. We won't halt percolation or the indexer in general if document percolation fails, actually indexing the documents is prioritised over percolation. No special care is taken to ensure that no events are missed when switching active indexes (re-indexing). Clients _can_ compensate for this by running comparisons against the event log, but subscriptions are primarily meant for non-critical use-cases like keeping data up-to-date in a UI.

### Flow

Indexers ask the coordinator to percolate the event/document after indexing it to the document index. The coordinator discards percolation requests from all but the active indexer. The coordinator saves the event ID and payload (computed fields + navigadoc) to database and notifies the percolator (pg pubsub) that there's new stuff to percolate (the payload gets cached in-process to cut down on db traffic). The percolator picks up on the notification and percolates events after the last percolated event up until the most recent. The result of the percolation is stored in DB and notifications are emitted. In-flight poll requests from clients react to the notification and responds with the results.

### Future development

Create a language neutral percolation index for each type and allow percolation documents to be created ahead of time for subscriptions that have been marked as language-neutral. This would cut down on the work needed (less stored percolation documents), and move percolation document creation out of the percolation loop (lower percolation latencies). Variant on this idea: subscriptions with a fixed language used to the same effect.

Stop percolation processing on high throughput, if for example a migration is running for millions of documents it's just a waste of resources.

Use a counting bloom filters (f.ex. https://github.com/tylertreat/BoomFilters/blob/master/counting.go) to decide if to emit a not-matched event, right now we always have to emit not matched. A probalistic best effort of not OK would be better.

Percolation concurrency - the percolation is currently completely serial and non-batched. In high throughput scenarios this makes it likely to start lagging. One low hanging fruit is batching documents in the percolation calls, another is to allow percolation for different types to run concurrently.

## Re-indexing

Re-indexing is performed by creating a new index set and re-indexing all documents into it. The new index set can be placed in another cluster than the currently active index set to avoid any performance degradation. Example request:

``` http
POST /twirp/elephant.index.Management/Reindex

{"cluster":"emerging-stranger"}

HTTP/1.1 200 OK

{
  "name": "magical-cottonmouth"
}
```

Reindexing into the new index set "magical-cottonmouth" will start immediately, and you can check index progress through metrics or by querying the status of the index sets:

``` http
POST /twirp/elephant.index.Management/ListIndexSets

{}

HTTP/1.1 200 OK

{
  "index_sets": [
    {
      "name": "awake-blockbuster",
      "cluster": "emerging-stranger",
      "enabled": true,
      "active": true,
      "position": "4263034"
    },
    {
      "name": "magical-cottonmouth",
      "cluster": "emerging-stranger",
      "enabled": true,
      "position": "372242"
    }
  ]
}
```

There we can see that we have some way to go before we have caught up with the currently active index set "awake-blockbuster". The management method `SetIndexSetStatus` can be used to pause the reindexing (by setting "enabled" to `true`), or set the new index as the currently active index:

``` http
POST /twirp/elephant.index.Management/SetIndexSetStatus

{
  "name": "magical-cottonmouth",
  "active": true
}

HTTP/1.1 200 OK

{}
```

This will route search traffic to the new index set. If the index set you're trying to activate lags behind the currently active index you will get an error response like this:

``` http
HTTP/1.1 412 Precondition Failed

{
  "code": "failed_precondition",
  "msg": "the index set lags behind with more than 10 events (3399619), use force_active to activate anyway"
}
```

Once the new index has been activated and verified to work: first call `SetIndexSetStatus` to disable the old index, then call `DeleteIndexSet` to delete it:

``` http
POST /twirp/elephant.index.Management/DeleteIndexSet

{
  "name": "awake-blockbuster"
}

HTTP/1.1 200 OK

{}
```
