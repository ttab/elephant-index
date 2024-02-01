-- name: Notify :exec
SELECT pg_notify(@channel::text, @message::text);

-- name: GetClusters :many
SELECT name, url, auth, created FROM cluster;

-- name: GetCluster :one
SELECT name, url, auth, created
FROM cluster
WHERE name = @name;

-- name: LockClusters :exec
LOCK TABLE cluster IN ACCESS EXCLUSIVE MODE;

-- name: GetClusterForUpdate :one
SELECT name, url, auth, created
FROM cluster
WHERE name = @name
FOR UPDATE;

-- name: ClusterIndexCount :one
SELECT
        COUNT(*) AS total,
        COUNT(*) FILTER (WHERE deleted = true) AS pending_delete
FROM index_set
WHERE cluster = sqlc.arg(cluster)::text;

-- name: ListClustersWithCounts :many
SELECT c.name, c.url, coalesce(i.c, 0) AS index_set_count
FROM cluster AS c
     LEFT JOIN (
          SELECT cluster, COUNT(*) AS c
          FROM index_set
          WHERE deleted = false
          GROUP BY cluster
     ) AS i ON i.cluster = c.name;

-- name: DeleteCluster :exec
DELETE FROM cluster WHERE name = @name;

-- name: AddCluster :exec
INSERT INTO cluster(name, url, auth) VALUES(@name, @url, @auth);

-- name: ListIndexSets :many
SELECT name FROM index_set WHERE deleted = false;

-- name: GetActiveIndexSet :one
SELECT name, position, cluster, active, enabled, deleted, modified
FROM index_set WHERE active = true;

-- name: GetIndexSets :many
SELECT name, position, cluster, active, enabled, deleted, modified
FROM index_set WHERE deleted = false;

-- name: IndexSetQuery :many
SELECT name, position, cluster, active, enabled, deleted, modified
FROM index_set WHERE deleted = false
AND (sqlc.narg(cluster)::text IS NULL OR cluster = @cluster)
AND (sqlc.narg(active)::bool IS NULL OR active = @active)
AND (sqlc.narg(enabled)::bool IS NULL OR enabled = @enabled)
LIMIT 10 OFFSET @row_offset;

-- name: GetIndexSet :one
SELECT name, position, cluster, active, enabled, deleted, modified
FROM index_set WHERE name = @name;

-- name: IndexSetExists :one
SELECT COUNT(*) = 1
FROM index_set
WHERE name = @name;

-- name: CreateIndexSet :exec
INSERT INTO index_set(
       name, position, cluster, active,
       enabled, modified
) VALUES (
       @name, @position, sqlc.arg(cluster)::text, @active,
       @enabled, NOW()
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

-- name: GetIndexSetForUpdate :one
SELECT name, position, cluster, active, enabled, deleted, modified
FROM index_set
WHERE name = @name
FOR UPDATE;

-- name: GetCurrentActiveForUpdate :one
SELECT name, position, cluster, active, enabled, deleted, modified
FROM index_set WHERE active = true
FOR UPDATE;

-- name: SetIndexSetStatus :exec
UPDATE index_set
SET active = @active, enabled = @enabled, deleted = @deleted, modified = NOW()
WHERE name = @name;

-- name: ListDeletedIndexSets :many
SELECT name
FROM index_set
WHERE deleted = true
FOR UPDATE NOWAIT;

-- name: GetIndexSetForDelete :one
SELECT name, position, cluster, active, enabled, deleted, modified
FROM index_set
WHERE name = @name AND deleted = true
FOR UPDATE NOWAIT;

-- name: DeleteIndexSet :exec
DELETE FROM index_set
WHERE name = @name AND deleted = true;
