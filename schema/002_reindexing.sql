CREATE TABLE IF NOT EXISTS cluster(
       name text NOT NULL PRIMARY KEY,
       url text NOT NULL,
       created timestamptz NOT NULL DEFAULT NOW()
);

ALTER TABLE index_set
      ADD COLUMN cluster text
          CONSTRAINT fk_set_cluster
                     REFERENCES cluster(name)
                     ON DELETE CASCADE,
      ADD COLUMN streaming boolean NOT NULL DEFAULT true,
      ADD COLUMN active boolean NOT NULL DEFAULT true,
      ADD COLUMN enabled boolean NOT NULL DEFAULT true,
      ADD COLUMN modified timestamptz NOT NULL DEFAULT NOW();

---- create above / drop below ----

DROP TABLE IF EXISTS cluster;

ALTER TABLE index_set
      DROP COLUMN cluster;
