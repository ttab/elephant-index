#!/usr/bin/env zsh

set -euo pipefail

ip=$(docker inspect repository-postgres | \
         jq -r '.[0].NetworkSettings.IPAddress')
url="postgres://indexer:pass@${ip}/indexer"

docker run --rm -it postgres pg_dump ${url} --schema-only \
       > postgres/schema.sql

docker run --rm -it postgres pg_dump ${url} --data-only \
       --column-inserts --table=schema_version \
       > postgres/schema_version.sql
