# Elephant index

## Local setup

Piggyback on the elephant postgres instance and create a user and database for the indexer:

``` sql
CREATE ROLE indexer WITH LOGIN PASSWORD 'pass';
CREATE DATABASE indexer WITH OWNER indexer;
```



