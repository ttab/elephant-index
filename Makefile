pg_conn?=postgres://indexer:pass@localhost/indexer
rollback_to?=0

UID := $(shell id -u)
GID := $(shell id -g)

SQL_TOOLS := ghcr.io/ttab/elephant-sqltools:v0.1.3

SQLC := docker run --rm \
	-v "${PWD}:/usr/src" -u $(UID):$(GID) \
	$(SQL_TOOLS) sqlc
TERN := docker run --rm \
	-v "${PWD}:/usr/src" \
	--network host \
	$(SQL_TOOLS) tern

.PHONY: db-rollback
db-rollback:
	$(TERN) migrate --migrations schema \
		--conn-string $(pg_conn) --destination $(rollback_to)

.PHONY: migrate
db-migrate: 
	$(TERN) migrate --migrations schema \
		--conn-string $(pg_conn)
	rm -f postgres/schema.sql
	make postgres/schema.sql

.PHONY: generate-sql
generate-sql: postgres/schema.sql postgres/queries.sql.go

postgres/schema.sql postgres/schema_version.sql:
	./dump-postgres-schema.sh

postgres/queries.sql.go: bin/sqlc postgres/schema.sql postgres/queries.sql
	$(SQLC) --experimental generate

.PHONY: docker-image
docker-image:
	docker build -t registry.a.tt.se/ttab/elephant-index --build-arg COMMAND=index .
