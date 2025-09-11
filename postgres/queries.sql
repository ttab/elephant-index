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
SELECT c.name, c.url, c.auth, coalesce(i.c, 0) AS index_set_count
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
SELECT name, position, cluster, active, enabled, deleted, modified, caught_up
FROM index_set WHERE active = true;

-- name: GetIndexSets :many
SELECT name, position, cluster, active, enabled, deleted, modified, caught_up
FROM index_set WHERE deleted = false;

-- name: IndexSetQuery :many
SELECT name, position, cluster, active, enabled, deleted, modified
FROM index_set WHERE deleted = false
AND (sqlc.narg(cluster)::text IS NULL OR cluster = @cluster)
AND (sqlc.narg(active)::bool IS NULL OR active = @active)
AND (sqlc.narg(enabled)::bool IS NULL OR enabled = @enabled)
LIMIT 10 OFFSET @row_offset;

-- name: GetIndexSet :one
SELECT name, position, cluster, active, enabled, deleted, modified, caught_up
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
SET position = @position, modified = NOW(), caught_up = @caught_up
WHERE name = @name;

-- name: GetIndexSetPosition :one
SELECT position, caught_up
FROM index_set
WHERE name = @name;

-- name: GetIndexConfiguration :one
SELECT mappings, feature_flags
FROM document_index
WHERE name = @name
FOR UPDATE;

-- name: GetMappingsForType :many
SELECT name, mappings
FROM document_index
WHERE set_name = @set_name
      AND content_type = @content_type;

-- name: UpdateIndexMappings :exec
UPDATE document_index
SET mappings = @mappings
WHERE name = @name;

-- name: CreateDocumentIndex :exec
INSERT INTO document_index(name, set_name, content_type, mappings, feature_flags)
VALUES (@name, @set_name, @content_type, @mappings, @feature_flags);

-- name: GetIndexSetForUpdate :one
SELECT name, position, cluster, active, enabled, deleted, modified, caught_up
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

-- name: CheckForPercolator :one
SELECT id FROM percolator
WHERE doc_type = @doc_type
      AND hash = @hash
      AND (
          owner = @owner OR (@owner IS NULL AND owner IS NULL));

-- name: CreatePercolator :one
INSERT INTO percolator(hash, owner, created, doc_type, query)
VALUES(@hash, @owner, @created, @doc_type, @query)
RETURNING id;

-- TODO: add pagination
-- name: GetPercolators :many
SELECT id, hash, owner, created, doc_type, query, deleted
FROM percolator
WHERE deleted = false;

-- name: GetPercolator :one
SELECT id, hash, owner, created, doc_type, query, deleted FROM percolator
WHERE id = @id AND deleted = false;

-- name: InsertPercolatorEvents :exec
INSERT INTO percolator_event(id, document, percolator, matched, created) (
       SELECT @id::bigint,
              @document::uuid,
              unnest(@percolators::bigint[]),
              unnest(@matched::bool[]),
              @created::timestamptz
) ON CONFLICT (id, percolator) DO NOTHING;

-- name: FetchPercolatorEvents :many
WITH p AS (
     SELECT unnest(@ids::bigint[]) AS id,
            unnest(@percolators::bigint[]) AS percolator
)
SELECT sub.id, sub.percolator, sub.matched FROM (
       -- We're only interested in the latest event for each document and
       -- percolator, so dedupe using window func. This also means that limit is
       -- applied pre-deduplication.
       SELECT e.id, e.percolator, e.matched,
              ROW_NUMBER() OVER (PARTITION BY e.document, e.percolator ORDER BY e.id DESC) AS rownum
       FROM percolator_event AS e
            INNER JOIN p ON e.id > p.id AND e.percolator = p.percolator
       ORDER by e.id ASC
       LIMIT sqlc.arg('limit')::bigint
) AS sub
WHERE sub.rownum = 1;

-- name: InsertPercolatorEventPayload :exec
INSERT INTO percolator_event_payload(
       id, created, data
) VALUES (
       @id, @created, @data
)
ON CONFLICT (id) DO NOTHING;

-- name: GetLastPercolatorEventID :one
SELECT COALESCE(MAX(id), 0)::bigint FROM percolator_event_payload;

-- name: GetPercolatorEventPayload :one
SELECT id, created, data
FROM percolator_event_payload
WHERE id = @id;

-- name: CreateSubscription :one
INSERT INTO subscription(
       percolator, client, hash, touched, spec
) VALUES (
       @percolator, @client, @hash, @touched, @spec
)
ON CONFLICT (percolator, client, hash) DO UPDATE
   SET touched = @touched
RETURNING id;

-- name: GetSubscriptions :many
SELECT id, percolator, spec
FROM subscription
WHERE id = ANY(@subscriptions::bigint[])
      AND client = @client;

-- name: TouchSubscriptions :exec
UPDATE subscription
       SET touched = @touched
WHERE id = ANY(@ids::bigint[]);

-- name: DropSubscription :exec
DELETE FROM subscription
WHERE percolator = @percolator
      AND client = @client;

-- name: GetActiveSubscriptionCount :one
SELECT COUNT(*) FROM subscription
WHERE percolator = @percolator AND touched > @cutoff;

-- name: PercolatorsToDelete :many
SELECT id FROM percolator AS p
WHERE NOT EXISTS (
      SELECT 1 FROM subscription AS s
      WHERE s.percolator = p.id
);

-- name: DeletePercolator :exec
DELETE FROM percolator
WHERE id = @id;

-- name: MarkPercolatorsForDeletion :exec
UPDATE percolator SET deleted = true
WHERE id = ANY(@ids::bigint[]);

-- name: GetPercolatorsMarkedForDeletion :many
SELECT id, doc_type FROM percolator WHERE deleted = true;

-- name: RegisterPercolatorDocumentIndex :exec
INSERT INTO percolator_document_index(percolator, index)
VALUES(@percolator, @index)
ON CONFLICT(percolator, index) DO NOTHING;

-- name: GetPercolatorDocumentIndices :many
SELECT index FROM percolator_document_index WHERE percolator = @percolator;

-- name: SubscriptionsToDelete :many
SELECT id FROM subscription
WHERE touched < @cutoff;

-- name: DeleteSubscriptions :exec
DELETE FROM subscription
WHERE id = ANY(@ids::bigint[]);

-- name: DeleteSubscriptionsForPercolators :exec
DELETE FROM subscription
WHERE percolator = ANY(@percolators::bigint[]);

-- name: DeletePercolatorEvents :exec
DELETE FROM percolator_event WHERE created < @cutoff;

-- name: DeletePercolatorEventPayloads :exec
DELETE FROM percolator_event_payload WHERE created < @cutoff;

-- name: SetAppState :exec
INSERT INTO app_state(name, data)
VALUES (@name, @data)
ON CONFLICT (name) DO UPDATE SET
   data = excluded.data;

-- name: GetAppState :one
SELECT data FROM app_state WHERE name = @name;
