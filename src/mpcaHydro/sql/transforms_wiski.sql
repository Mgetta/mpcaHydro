CREATE OR REPLACE VIEW analytics.wiski AS

WITH unit_converted AS (
    SELECT *,
        CASE
            WHEN LOWER(ts_unitsymbol) = '°c'   THEN (Value * 9/5) + 32
            WHEN LOWER(ts_unitsymbol) = 'kg'    THEN Value * 2.20462
            ELSE Value
        END AS converted_value,
        CASE
            WHEN LOWER(ts_unitsymbol) = '°c'    THEN 'degf'
            WHEN LOWER(ts_unitsymbol) = 'kg'     THEN 'lb'
            WHEN LOWER(ts_unitsymbol) = 'ft³/s'  THEN 'cfs'
            ELSE ts_unitsymbol
        END AS unit
    FROM staging.wiski
),

normalized AS (
    SELECT
        "station_no" AS station_id,
        m.constituent,

        -- Force cast these to standard timezone-naive types
        CAST("Timestamp" AS TIMESTAMP) AS datetime,
        CAST("Date" AS DATE) AS date,
        CAST("Time" AS TIME) AS time,

        "Timezone" AS timezone,
        converted_value AS value,
        unit,
        "Quality Code" AS quality_code,
        'wiski' AS station_origin
    FROM unit_converted u
    JOIN mappings.wiski_parametertype m
        ON u.parametertype_id = m.parametertype_id
),

quality_filtered AS (
    SELECT 
        n.*
    FROM normalized n
    INNER JOIN mappings.wiski_quality_codes wqc 
        ON n.quality_code = wqc.quality_code
    WHERE wqc.active = 1
),


--year_filtered AS (
--    SELECT *
--    FROM quality_filtered
--    WHERE year(datetime) >= getvariable('min_year')
--),

-- hourly_averaged AS (
--     SELECT
--         station_id, constituent,
--         DATE_TRUNC('hour', datetime + INTERVAL '30 minute') AS datetime,
--         AVG(value) AS value,
--         unit, station_origin
--     FROM quality_filtered
--     GROUP BY station_id, constituent, datetime, unit, station_origin
-- )
hourly_averaged AS (
    -- Step 7: average_results
    SELECT
        station_id, 
        constituent,
        date,
        -- If time is NOT NULL (sub-daily), round it to the nearest hour. 
        -- If time IS NULL (daily), leave it as NULL.
        CASE 
            WHEN time IS NOT NULL THEN 
                -- Combine date and time, round to nearest hour, and extract the TIME back out
                CAST(DATE_TRUNC('hour', (date + time) + INTERVAL '30 minute') AS TIME)
            ELSE NULL 
        END AS time,
        
        AVG(value) AS value,
        unit, 
        station_origin,
        (date + time) AS datetime
    FROM quality_filtered
    
    GROUP BY 
        station_id, 
        constituent, 
        date, 
        time, 
        unit, 
        station_origin
)

SELECT * FROM hourly_averaged;