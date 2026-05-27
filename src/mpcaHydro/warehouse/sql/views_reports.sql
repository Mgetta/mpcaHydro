-- views_reports.sql
-- Views for the reports schema

-- View: wiski_qc_count
-- Quality code counts for WISKI data
CREATE OR REPLACE VIEW reports.wiski_qc_count AS (
    SELECT 
        w.station_no,
        w.parametertype_name,
        w."Quality Code",
        COUNT(w."Quality Code") AS count,
        wqc."Text",
        wqc.Description
    FROM staging.wiski w 
    LEFT JOIN mappings.wiski_quality_codes wqc
        ON w."Quality Code" = wqc.quality_code
    --WHERE wqc.Active = 1
    GROUP BY
        w."Quality Code", wqc."Text", wqc.Description, w.parametertype_name, w.station_no
);

-- View: constituent_summary
-- Summary of constituents across all stations
CREATE OR REPLACE VIEW reports.constituent_summary AS
SELECT
    station_id,
    station_origin,
    constituent,
    COUNT(*) AS sample_count,
    AVG(value) AS average_value,
    MIN(value) AS min_value,
    MAX(value) AS max_value,
    year(MIN(date)) AS start_date,
    year(MAX(date)) AS end_date
FROM
    analytics.observations
GROUP BY
    constituent, station_id, station_origin;

-- View: outlet_constituent_summary
-- Summary of constituents by outlet
CREATE OR REPLACE VIEW reports.outlet_constituent_summary AS
SELECT
    outlet_id,
    constituent,
    count_star() AS sample_count,
    avg("value") AS average_value,
    min("value") AS min_value,
    max("value") AS max_value,
    "year"(min(date)) AS start_date,
    "year"(max(date)) AS end_date
FROM
    analytics.outlet_observations
GROUP BY
    constituent,
    outlet_id;
