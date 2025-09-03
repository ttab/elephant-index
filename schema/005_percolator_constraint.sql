ALTER TABLE percolator DROP CONSTRAINT IF EXISTS pcl_unique_hash;
ALTER TABLE percolator ADD CONSTRAINT pcl_unique_hash UNIQUE NULLS NOT DISTINCT(doc_type, hash, owner);

---- create above / drop below ----

ALTER TABLE percolator DROP CONSTRAINT IF EXISTS pcl_unique_hash;
ALTER TABLE percolator ADD CONSTRAINT pcl_unique_hash UNIQUE NULLS NOT DISTINCT(hash, owner);
