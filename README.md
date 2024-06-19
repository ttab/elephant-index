# Elephant index

Elephant index follows the event log from the elephant repository and indexes the updated documents in OpenSearch.

Before indexing, documents are flattened to a [flat property structure](index/testdata/raw_1.values.json) and the [revisor](https://github.com/ttab/revisor) schemas of the repository are used together with the document data to construct the [mappings for the open search index](index/testdata/raw_1.mappings.json).

The indexer will add to the index mappings as needed, and no new properties will be indexed without first creating a mapping for it. A separate index is created for each document type and language. This lets us avoid conflicts between mappings of separate document types, and allows us to use language specific analyzers for better multilingual support.

## Searching

Elephant index proxies search requests to OpenSearch with some extra processing. First the index name is mapped to the currently active index(es) for the specified type(s). Then, we check that the client has the "search" scope, and then incoming query is wrapped in a boolean query that enforces that only documents that has the client subject or one of the clients units in its allowed `readers` are searchable.

### Index names

The elephant index creates indexes in the pattern `documents-[random name]-[type]-[language]`. So if the name of the index set is "factual-tiger", the type of the document is "core/article", and the language is "sv-se", the following index will be used: "documents-factual-tiger-core_article-sv-se". For language codes without region the suffix "-unspecified" will be used, e.g. "sv" -> "documents-factual-tiger-core_article-sv-unspecified".

Incoming searches will only be able to specify the part of the index name that comes after "documents-factual-tiger-" as the first part depends on internal indexer state. The reason for having a index set part of the index name is that that will be used when re-indexing. When re-indexing a new index set will be created, and the event-log will be replayed to populate those indexes. When the new indexes have caught up we can switch over to using the new indexes. 

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
