-- views_analytics.sql
-- Views for the analytics schema

-- View: wiski_normalized
-- Normalized WISKI data with unit conversions and column renames
-- CREATE OR REPLACE VIEW analytics.wiski_normalized AS
-- SELECT 
--     -- Convert °C to °F and keep other values unchanged
--     CASE 
--         WHEN LOWER(ts_unitsymbol) = '°c' THEN (value * 9.0 / 5.0) + 32
--         WHEN ts_unitsymbol = 'kg' THEN value * 2.20462
--         ELSE value
--     END AS value,

--     -- Normalize units
--     CASE 
--         WHEN LOWER(ts_unitsymbol) = '°c' THEN 'degf'
--         WHEN ts_unitsymbol = 'kg' THEN 'lb'
--         WHEN ts_unitsymbol = 'ft³/s' THEN 'cfs'
--         ELSE ts_unitsymbol
--     END AS unit,

--     -- Normalize column names
--     station_no AS station_id,
--     Timestamp AS datetime,
--     "Quality Code" AS quality_code,
--     "Quality Code Name" AS quality_code_name,
--     parametertype_id,
--     constituent
-- FROM staging.wiski;

-- View: observations
-- Combined observations from equis and wiski processed tables
CREATE OR REPLACE VIEW analytics.observations AS
SELECT datetime,date,time, value, station_id, station_origin, constituent, unit
FROM analytics.equis
UNION ALL
SELECT datetime,date,time, value, station_id, station_origin, constituent, unit
FROM analytics.wiski
UNION ALL
SELECT datetime,date,time, value, station_id,station_origin, constituent, unit
FROM derived.baseflow;


-- View: outlet_observations
-- Links observations to model reaches via outlets
CREATE OR REPLACE VIEW analytics.outlet_observations AS 
SELECT
    o.date,
    o.time,
    o.datetime,
    os.outlet_id,
    o.constituent,
    AVG(o.value) AS value,
    COUNT(o.value) AS count
FROM
    analytics.observations AS o
INNER JOIN
    outlets.outlet_stations AS os 
    ON o.station_id = os.station_id AND o.station_origin = os.station_origin
WHERE os.outlet_id IS NOT NULL
GROUP BY
    os.outlet_id,
    o.constituent,
    o.unit,
    o.date,
    o.time,
    o.datetime;


CREATE OR REPLACE VIEW analytics.outlet_observations_with_flow AS
WITH 
    -- 1. "Daily Era" Flow (Sensors recording only once a day)
    daily_flow AS (
        SELECT 
            outlet_id, 
            date,
            AVG(CASE WHEN constituent = 'Q' THEN "value" END) as daily_flow,
            AVG(CASE WHEN constituent = 'QB' THEN "value" END) as daily_baseflow
        FROM analytics.outlet_observations
        WHERE time IS NULL 
          AND constituent IN ('Q', 'QB') -- Only look at flow here
        GROUP BY 1, 2
    ),

    -- 2. "Sub-Daily Era" Flow 
    subdaily_flow AS (
        SELECT 
            outlet_id,
            date, 
            date_trunc('hour', datetime) AS hour_bucket,
            AVG(CASE WHEN constituent = 'Q' THEN "value" END) AS subdaily_flow,
            AVG(CASE WHEN constituent = 'QB' THEN "value" END) AS subdaily_baseflow
        FROM analytics.outlet_observations
        WHERE time IS NOT NULL 
          AND constituent IN ('Q', 'QB') -- Only look at flow here
        GROUP BY 1, 2, 3
    ),

    -- 3. Chemistry Samples (Do NOT group or aggregate these!)
    chemistry AS (
        SELECT 
            outlet_id,
            datetime,
            date,
            time,
            -- Create a bucket solely for joining to the sub-daily flow
            date_trunc('hour', datetime) AS hour_bucket, 
            constituent,
            "value" AS constituent_value
        FROM analytics.outlet_observations
        WHERE constituent NOT IN ('Q', 'QB')
    )

-- 4. Bring it all together
SELECT 
    c.outlet_id,
    c.datetime,
    c.date,
    c.constituent,
    c.constituent_value AS value,
    
    -- The Magic: Seamlessly transitions between hardware eras
    COALESCE(sf.subdaily_flow, df.daily_flow) AS flow_value,
    COALESCE(sf.subdaily_baseflow, df.daily_baseflow) AS baseflow_value

FROM chemistry c

-- Join the hourly flow bucket to the chemistry sample
LEFT JOIN subdaily_flow sf
    ON c.outlet_id = sf.outlet_id 
    AND c.hour_bucket = sf.hour_bucket

-- Join the daily flow fallback
LEFT JOIN daily_flow df
    ON c.outlet_id = df.outlet_id 
    AND c.date = df.date;