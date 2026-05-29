CREATE SCHEMA IF NOT EXISTS outlets;

CREATE TABLE IF NOT EXISTS outlets.outlet_groups (
    outlet_id INTEGER PRIMARY KEY,
    repository_name TEXT NOT NULL,
    outlet_name TEXT,
    notes TEXT
);

CREATE TABLE IF NOT EXISTS outlets.outlet_stations (
    outlet_id INTEGER NOT NULL,
    station_id TEXT NOT NULL,
    station_origin TEXT NOT NULL,
    repository_name TEXT NOT NULL,
    true_opnid INTEGER,
    wplmn_flag INTEGER NOT NULL DEFAULT 0,
    comments TEXT,
    CONSTRAINT uq_station_origin UNIQUE (station_id, station_origin),
    FOREIGN KEY (outlet_id) REFERENCES outlets.outlet_groups(outlet_id)
);

CREATE TABLE IF NOT EXISTS outlets.outlet_reaches (
    outlet_id INTEGER NOT NULL,
    reach_id INTEGER NOT NULL,
    repository_name TEXT NOT NULL,
    PRIMARY KEY (outlet_id, reach_id),
    FOREIGN KEY (outlet_id) REFERENCES outlets.outlet_groups(outlet_id)
);

CREATE OR REPLACE VIEW outlets.station_reach_pairs AS
SELECT
    s.outlet_id,
    s.station_id,
    s.station_origin,
    r.reach_id,
    r.repository_name
FROM outlets.outlet_stations s
JOIN outlets.outlet_reaches r USING (outlet_id);

CREATE OR REPLACE VIEW outlets.stations AS
SELECT
    s.outlet_id,
    s.station_id,
    s.station_origin    AS source,
    s.repository_name   AS repo_name,
    s.true_opnid,
    s.wplmn_flag,
    s.comments,
    r.reach_id          AS opnids   -- single int per row, matches old semantics
FROM outlets.outlet_stations s
LEFT JOIN outlets.outlet_reaches r USING (outlet_id);