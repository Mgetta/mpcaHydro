

-- -- staging.wiski schema copied for baseflow processing
-- CREATE OR REPLACE VIEW derived.baseflow AS 
-- SELECT 
--     CAST(NULL AS DATETIME) AS "Timestamp",
--     CAST(NULL AS DATE) AS "Date",
--     CAST(NULL AS TIME) AS "Time",
--     CAST(NULL AS DOUBLE) AS "Value",
--     CAST(NULL AS BIGINT) AS "Quality Code",
--     CAST(NULL AS VARCHAR) AS "Quality Code Name",
--     CAST(NULL AS VARCHAR) AS ts_unitsymbol,
--     CAST(NULL AS VARCHAR) AS ts_id,
--     CAST(NULL AS VARCHAR) AS station_no,
--     CAST(NULL AS VARCHAR) AS station_name,
--     CAST(NULL AS VARCHAR) AS station_latitude,
--     CAST(NULL AS VARCHAR) AS station_longitude,
-- WHERE FALSE;


CREATE OR REPLACE VIEW derived.baseflow AS 
SELECT 
    CAST(NULL AS DATETIME) AS datetime,
    CAST(NULL AS DATE) AS date,
    CAST(NULL AS TIME) AS time,
    CAST(NULL AS DOUBLE) AS value,
    CAST(NULL AS VARCHAR) AS station_id,
    CAST(NULL AS VARCHAR) AS station_origin,
    CAST(NULL AS VARCHAR) AS constituent,
    CAST(NULL AS VARCHAR) AS unit,
    CAST(NULL AS VARCHAR) AS grain,
    CAST(NULL AS VARCHAR) AS statistic,
    CAST(NULL AS VARCHAR) AS interval_minutes
WHERE FALSE;
-- CREATE OR REPLACE TABLE derived.baseflow (
--     datetime TIMESTAMP,
--     value DOUBLE,
--     station_id VARCHAR,
--     station_origin VARCHAR,
--     constituent VARCHAR,
--     unit VARCHAR
-- );