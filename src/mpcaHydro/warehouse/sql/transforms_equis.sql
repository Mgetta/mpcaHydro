CREATE OR REPLACE VIEW analytics.equis AS

WITH mapped AS (
    -- Step 1: map_constituents
    SELECT
        e.*,
        m.constituent
    FROM staging.equis e
    JOIN mappings.equis_casrn m ON e.CAS_RN = m.cas_rn
),

timezone_normalized AS (
    -- Step 2: normalize_timezone to UTC-6
    SELECT * ,
        CASE
            WHEN SAMPLE_DATE_TIMEZONE = 'CST' THEN SAMPLE_DATE_TIME
            WHEN SAMPLE_DATE_TIMEZONE = 'CDT' THEN SAMPLE_DATE_TIME - INTERVAL '1 hour'
            WHEN SAMPLE_DATE_TIMEZONE = 'UTC' THEN SAMPLE_DATE_TIME - INTERVAL '6 hour'
            ELSE SAMPLE_DATE_TIME -- If timezone is missing or unrecognized assume it's already in UTC-6
        END AS datetime
    FROM mapped
),
unit_converted AS (
    -- Step 3: convert_units
    SELECT *,
        CASE
            WHEN LOWER(RESULT_UNIT) = 'ug/l'           THEN RESULT_NUMERIC / 1000
            WHEN LOWER(RESULT_UNIT) = 'mg/g'            THEN RESULT_NUMERIC * 1000
            WHEN LOWER(RESULT_UNIT) IN ('deg c', 'degc') THEN (RESULT_NUMERIC * 9/5) + 32
            ELSE RESULT_NUMERIC
        END AS value,
        CASE
            WHEN LOWER(RESULT_UNIT) = 'ug/l'           THEN 'mg/L'
            WHEN LOWER(RESULT_UNIT) = 'mg/g'            THEN 'mg/L'
            WHEN LOWER(RESULT_UNIT) IN ('deg c', 'degc') THEN 'degF'
            ELSE RESULT_UNIT
        END AS unit
    FROM timezone_normalized
),


columns_normalized AS (
    -- Step 4: normalize_columns
    SELECT
        SYS_LOC_CODE AS station_id,
        constituent,
        value,
        unit,
        'equis' AS station_origin,
        CAST(datetime AS DATE) AS date,
        CAST(datetime AS TIME) AS time,
        DETECT_FLAG as detect_flag,
        REPORTING_DETECTION_LIMIT as reporting_detection_limit,
        SAMPLE_METHOD as sample_method,
        grain,
        statistic,
        interval_minutes
    FROM unit_converted
),
    


nondetects_replaced AS (
    -- Step 5: flag and replace_nondetects with 1/2 the detection limit
    SELECT *,
        CASE
            WHEN detect_flag = 'N' THEN CAST(reporting_detection_limit AS FLOAT) / 2.0
            ELSE value
        END AS value
    --- COALESCE(value, 0) AS value, -- Optionally replace NULLs with 0, or you could choose to leave them as NULL
    FROM columns_normalized
),


sample_method_filtered AS (
    SELECT 
        n.*
    FROM nondetects_replaced n
    LEFT JOIN mappings.equis_sample_methods esm 
        ON n.sample_method = esm.sample_method
    WHERE esm.include = 1
),


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
        grain,
        'MEAN' AS statistic,
        60 AS interval_minutes,
        station_origin,
        COALESCE(date + time, date::TIMESTAMP) AS datetime
    FROM sample_method_filtered
    
    GROUP BY 
        station_id, 
        constituent, 
        date, 
        time, 
        unit, 
        grain,
        statistic,
        interval_minutes,
        station_origin
)


SELECT * FROM hourly_averaged;