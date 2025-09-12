ALTER TABLE percolator
      ADD COLUMN language text NOT NULL DEFAULT '',
      DROP CONSTRAINT pcl_unique_hash,
      ADD CONSTRAINT pcl_unique_hash UNIQUE NULLS NOT DISTINCT(doc_type, language, hash, owner);

---- create above / drop below ----

ALTER TABLE percolator
      DROP COLUMN language text NOT NULL DEFAULT '',
      DROP CONSTRAINT pcl_unique_hash,
      ADD CONSTRAINT pcl_unique_hash UNIQUE NULLS NOT DISTINCT(hash, owner);
