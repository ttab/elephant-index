-- name: Notify :exec
SELECT pg_notify(@channel::text, @message::text);

-- name: GetClusters :many
SELECT name, url FROM cluster;

-- name: AddCluster :exec
INSERT INTO cluster(name, url) VALUES(@name, @url);

-- name: ListIndexSets :many
SELECT name FROM index_set;

-- name: GetIndexSets :many
SELECT name, position, cluster, streaming, active, enabled, modified
FROM index_set;

-- name: CreateIndexSet :exec
INSERT INTO index_set(name, position, cluster, streaming, active, enabled, modified)
VALUES (@name, @position, sqlc.arg(cluster)::text, @streaming, @active, @enabled, NOW());

-- name: DeleteOldIndexSets :exec
DELETE FROM index_set
WHERE modified < @cutoff
AND NOT active AND NOT enabled;

-- name: DeleteUnusedClusters :exec
DELETE FROM cluster AS c
WHERE c.created < @cutoff
AND NOT EXISTS (
    SELECT FROM index_set AS s
    WHERE s.cluster = c.name
);

-- name: SetClusterWhereMissing :exec
UPDATE index_set
SET cluster = sqlc.arg(cluster)::text
WHERE cluster IS NULL;

-- name: UpdateSetPosition :exec
UPDATE index_set
SET position = @position, modified = NOW()
WHERE name = @name;

-- name: GetIndexSetPosition :one
SELECT position
FROM index_set
WHERE name = @name;

-- name: GetIndexMappings :one
SELECT mappings
FROM document_index
WHERE name = @name
FOR UPDATE;

-- name: UpdateIndexMappings :exec
UPDATE document_index
SET mappings = @mappings
WHERE name = @name;

-- name: CreateDocumentIndex :exec
INSERT INTO document_index(name, set_name, content_type, mappings)
VALUES (@name, @set_name, @content_type, @mappings);

-- name: GetForActivation :many
SELECT name, position, cluster, streaming, active, enabled, modified
FROM index_set
WHERE active OR name = @name
FOR UPDATE;

-- name: SetActive :exec
UPDATE index_set
SET active = @active, modified = NOW()
WHERE name = @name;

