-- H3 Features Table Schema (v3 with Resolution)
-- Run this as h3expert: psql -U h3expert -d h3dbtest -f src/features_schema.sql

DROP TABLE IF EXISTS h3_features;

CREATE TABLE h3_features (
    id TEXT PRIMARY KEY,
    geom GEOMETRY(POLYGON, 4326),
    color INTEGER,
    resolution INTEGER
);

-- Spatial index for high-performance mapping
CREATE INDEX h3_features_geom_idx ON h3_features USING GIST (geom);

-- B-Tree index for resolution filtering
CREATE INDEX h3_features_res_idx ON h3_features (resolution);
