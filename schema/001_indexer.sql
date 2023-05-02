CREATE TABLE IF NOT EXISTS index_set(
       name text not null primary key,
       position bigint not null
);

CREATE TABLE IF NOT EXISTS document_index(
       name text not null primary key,
       set_name text not null,
       content_type text not null,
       mappings jsonb not null,
       FOREIGN KEY(set_name) REFERENCES index_set(name)
               ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS document_index_content_type_idx ON document_index(content_type);

CREATE TABLE IF NOT EXISTS indexing_override(
       content_type text not null,
       field text not null,
       mapping jsonb not null,
       primary key(content_type, field)
);

CREATE TABLE IF NOT EXISTS job_lock(
       name text not null primary key,
       holder text not null,
       touched timestamptz not null,
       iteration bigint not null
);

---- create above / drop below ----

DROP INDEX IF EXISTS document_index_content_type_idx;

DROP TABLE IF EXISTS document_index;

DROP TABLE IF EXISTS indexing_override;

DROP TABLE IF EXISTS index_set;

DROP TABLE IF EXISTS job_lock;
