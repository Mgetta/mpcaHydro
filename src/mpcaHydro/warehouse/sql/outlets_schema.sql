-- outlets_schema.sql
-- Schema for managing associations between model reaches and observation stations via outlets
-- Compatible with DuckDB and SQLite

CREATE SCHEMA IF NOT EXISTS outlets;

CREATE TABLE IF NOT EXISTS outlets.stations (
    station_id TEXT,
    source TEXT,
    repo_name TEXT,
    true_opnid INTEGER,
    opnids INTEGER,
    outlet_id INTEGER,
    wplmn_flag INTEGER,
    modeled INTEGER,
    comments TEXT
);

-- Table 1: outlet_groups
-- Represents a logical grouping that ties stations and reaches together
-- CREATE TABLE IF NOT EXISTS outlets.outlet_groups (
--     outlet_id INTEGER PRIMARY KEY,
--     repository_name TEXT NOT NULL,
--     outlet_name TEXT,
--     notes TEXT
-- );


CREATE OR REPLACE VIEW outlets.outlet_groups AS
SELECT 
    outlet_id,
    MAX(repo_name) AS repository_name,
    NULL AS outlet_name,
    NULL AS notes
FROM outlets.stations
WHERE outlet_id IS NOT NULL
GROUP BY outlet_id;



-- Table 2: outlet_stations
-- One-to-many: outlet -> stations
-- CREATE TABLE IF NOT EXISTS outlets.outlet_stations (
--     outlet_id INTEGER NOT NULL,
--     station_id TEXT NOT NULL,
--     station_origin TEXT NOT NULL,
--     repository_name TEXT NOT NULL,
--     true_opnid INTEGER NOT NULL,
--     wplmn_flag INTEGER NOT NULL DEFAULT 0,   -- ← add this
--     comments TEXT,
--     CONSTRAINT uq_station_origin UNIQUE (station_id, station_origin),
--     FOREIGN KEY (outlet_id) REFERENCES outlets.outlet_groups(outlet_id)
-- );
CREATE OR REPLACE VIEW outlets.outlet_stations AS
SELECT DISTINCT
    ANY_VALUE(outlet_id) AS outlet_id,
    station_id,
    source AS station_origin,
    ANY_VALUE(repo_name) AS repository_name,
    ANY_VALUE(true_opnid) AS true_opnid,
    ANY_VALUE(wplmn_flag) AS wplmn_flag,
    ANY_VALUE(comments) AS comments
FROM outlets.stations
WHERE outlet_id IS NOT NULL
GROUP BY
    station_id,
    source;



-- Table 3: outlet_reaches
-- One-to-many: outlet -> reaches
-- A reach can appear in multiple outlets, enabling many-to-many overall
-- CREATE TABLE IF NOT EXISTS outlets.outlet_reaches (
--     outlet_id INTEGER NOT NULL,
--     reach_id INTEGER NOT NULL,
--     repository_name TEXT NOT NULL,
--     FOREIGN KEY (outlet_id) REFERENCES outlets.outlet_groups(outlet_id)
-- );

CREATE OR REPLACE VIEW outlets.outlet_reaches AS
SELECT DISTINCT 
    outlet_id,
    opnids AS reach_id,
    repo_name AS repository_name
FROM outlets.stations
WHERE opnids IS NOT NULL;


-- Useful views:

-- View: station_reach_pairs
-- Derives the implicit many-to-many station <-> reach relationship via shared outlet_id
CREATE OR REPLACE VIEW outlets.station_reach_pairs AS
SELECT
  s.outlet_id,
  s.station_id,
  s.station_origin,
  r.reach_id,
  r.repository_name
FROM outlets.outlet_stations AS s
JOIN outlets.outlet_reaches AS r
  ON s.outlet_id = r.outlet_id;

