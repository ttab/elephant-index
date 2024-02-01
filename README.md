# Elephant index

Elephant index follows the event log from the elephant repository and indexes the updated documents in OpenSearch.

Before indexing, documents are flattened to a [flat property structure](index/testdata/raw_1.values.json) and the [revisor](https://github.com/ttab/revisor) schemas of the repository are used together with the document data to construct the [mappings for the open search index](index/testdata/raw_1.mappings.json).

The indexer will add to the index mappings as needed, and no new properties will be indexed without first creating a mapping for it. A separate index is created for each document type and language. This lets us avoid conflicts between mappings of separate document types, and allows us to use language specific analyzers for better multilingual support.

## Searching

Elephant index proxies search requests to OpenSearch with some extra processing. First the index name is mapped to the currently active index(es) for the specified type(s). Then, we check that the client has the "search" scope, and then incoming query is wrapped in a boolean query that enforces that only documents that has the client subject or one of the clients units in its allowed `readers` are searchable.

### Index names

The elephant index creates indexes in the pattern `documents-[random name]-[type]-[language]`. So if the name of the index set is "factual-tiger", the type of the document is "core/article", and the language is "sv", the following index will be used: "documents-factual-tiger-core_article-sv".

Incoming searches will only be able to specify the part of the index name that comes after "documents-factual-tiger-" as the first part depends on internal indexer state. The reason for having a index set part of the index name is that that will be used when re-indexing. When re-indexing a new index set will be created, and the event-log will be replayed to populate those indexes. When the new indexes have caught up we can switch over to using the new indexes. 

## Local setup

Piggyback on the elephant postgres instance and create a user and database for the indexer:

``` sql
CREATE ROLE indexer WITH LOGIN PASSWORD 'pass';
CREATE DATABASE indexer WITH OWNER indexer;
```



