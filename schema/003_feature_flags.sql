ALTER TABLE document_index
      ADD COLUMN feature_flags text[];

---- create above / drop below ----

ALTER TABLE document_index
      DROP COLUMN feature_flags;
