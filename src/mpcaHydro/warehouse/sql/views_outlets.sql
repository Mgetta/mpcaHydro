-- views_outlets.sql
-- Views for the outlets schema

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
