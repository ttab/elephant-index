-- name: CreateIndexSet :exec
INSERT INTO index_set(name, position)
VALUES (@name, @position);

-- name: UpdateSetPosition :exec
UPDATE index_set
SET position = @position
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

-- name: CreateIndex :exec
INSERT INTO document_index(name, set_name, content_type, mappings)
VALUES (@name, @set_name, @content_type, @mappings);


