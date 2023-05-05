bin/sqlc: go.mod
	GOBIN=${PWD}/bin go install github.com/kyleconroy/sqlc/cmd/sqlc

pg/queries.sql.go: bin/sqlc pg/schema.sql pg/queries.sql
	./bin/sqlc --experimental generate
