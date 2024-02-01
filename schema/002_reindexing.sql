CREATE TABLE IF NOT EXISTS cluster(
       name text NOT NULL PRIMARY KEY,
       url text NOT NULL,
       auth jsonb NOT NULL,
       created timestamptz NOT NULL DEFAULT NOW()
);

ALTER TABLE index_set
      ADD COLUMN cluster text
          CONSTRAINT fk_set_cluster
                     REFERENCES cluster(name)
                     ON DELETE CASCADE,
      ADD COLUMN active boolean NOT NULL DEFAULT false,
      ADD COLUMN enabled boolean NOT NULL DEFAULT true,
      ADD COLUMN deleted boolean NOT NULL DEFAULT false,
      ADD COLUMN modified timestamptz NOT NULL DEFAULT NOW();

CREATE UNIQUE INDEX unique_single_active
ON index_set(active)
WHERE active = true;

---- create above / drop below ----

ALTER TABLE index_set
      DROP COLUMN cluster,
      DROP COLUMN active,
      DROP COLUMN enabled;

DROP TABLE IF EXISTS cluster;
