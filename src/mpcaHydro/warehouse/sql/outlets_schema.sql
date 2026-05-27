-- outlets_schema.sql
-- Schema for managing associations between model reaches and observation stations via outlets
-- Compatible with DuckDB and SQLite

CREATE SCHEMA IF NOT EXISTS outlets;

-- Table 1: outlet_groups
-- Represents a logical grouping that ties stations and reaches together
CREATE TABLE IF NOT EXISTS outlets.outlet_groups (
    outlet_id INTEGER PRIMARY KEY,
    repository_name TEXT NOT NULL,
    outlet_name TEXT,
    notes TEXT
);

-- Table 2: outlet_stations
-- One-to-many: outlet -> stations
CREATE TABLE IF NOT EXISTS outlets.outlet_stations (
    outlet_id INTEGER NOT NULL,
    station_id TEXT NOT NULL,
    station_origin TEXT NOT NULL,
    repository_name TEXT NOT NULL,
    true_opnid INTEGER NOT NULL,
    comments TEXT,
    CONSTRAINT uq_station_origin UNIQUE (station_id, station_origin),
    FOREIGN KEY (outlet_id) REFERENCES outlets.outlet_groups(outlet_id)
);

-- Table 3: outlet_reaches
-- One-to-many: outlet -> reaches
-- A reach can appear in multiple outlets, enabling many-to-many overall
CREATE TABLE IF NOT EXISTS outlets.outlet_reaches (
    outlet_id INTEGER NOT NULL,
    reach_id INTEGER NOT NULL,
    repository_name TEXT NOT NULL,
    FOREIGN KEY (outlet_id) REFERENCES outlets.outlet_groups(outlet_id)
);

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

