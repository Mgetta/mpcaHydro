-- The Station Dimension
CREATE TABLE IF NOT EXISTS dimensions.station (
    station_pk INTEGER PRIMARY KEY, -- Generated ID (1, 2, 3...)
    station_id TEXT,                -- 'E66050001'
    station_origin TEXT,            -- 'WISKI' or 'EQUIS'
    station_name TEXT,
    station_latitude FLOAT,
    station_longitude FLOAT

);

-- The Constituent Dimension (For your EQuIS data)
CREATE TABLE IF NOT EXISTS dimensions.constituent (
    constituent_pk INTEGER PRIMARY KEY,
    constituent_code TEXT,          -- 'TSS', 'TP', 'TN', etc.
    constituent_name TEXT,           -- 'Total Suspended Solids', 'Total Phosphorus', etc.
    unit TEXT,              -- 'mg/L', 'degf', etc.
    source_id TEXT,            -- Original ID from EQuIS, if applicable
    source_origin TEXT        -- 'EQUIS' or 'WISKI' to track where this constituent came from
);

-- continuous quality code dimension
CREATE TABLE IF NOT EXISTS dimensions.quality_code (
    quality_code_sk INTEGER PRIMARY KEY,
    quality_code TEXT,
    quality_code_name TEXT,
    description TEXT,
    active BOOLEAN,
    source_origin TEXT        -- 'USGS' or 'WISKI' to track where this quality code came from
);

CREATE SEQUENCE IF NOT EXISTS seq_statistic_sk;
CREATE TABLE IF NOT EXISTS dimensions.statistic (
    statistic_sk INTEGER DEFAULT nextval('seq_statistic_sk') PRIMARY KEY,
    statistic_code TEXT,       -- 'INST', 'MEAN', 'MAX', 'MIN', 'SUM'
    statistic_name TEXT,       -- 'Instantaneous', 'Daily Mean', etc.
    description TEXT
);




CREATE TABLE dimensions.date AS
WITH date_series AS (
    -- Generate every single day from 1980 to 2050
    SELECT unnest(generate_series(DATE '1980-01-01', DATE '2050-12-31', INTERVAL 1 DAY)) AS d
)
SELECT 
    -- The Smart Surrogate Key (YYYYMMDD)
    CAST(strftime(d, '%Y%m%d') AS INTEGER) AS date_sk,
    
    d AS full_date,
    
    -- Standard Calendar
    EXTRACT(YEAR FROM d) AS calendar_year,
    EXTRACT(MONTH FROM d) AS calendar_month,
    strftime(d, '%b') AS month_name_short,
    EXTRACT(DOY FROM d) AS day_of_year,
    
    -- Hydrology: USGS Water Year (Starts Oct 1st)
    CASE 
        WHEN EXTRACT(MONTH FROM d) >= 10 THEN EXTRACT(YEAR FROM d) + 1 
        ELSE EXTRACT(YEAR FROM d) 
    END AS water_year,
    
    -- Hydrology: Day of Water Year (Oct 1 = 1)
    CASE 
        WHEN EXTRACT(MONTH FROM d) >= 10 THEN EXTRACT(DOY FROM d) - EXTRACT(DOY FROM MAKE_DATE(EXTRACT(YEAR FROM d), 10, 1)) + 1
        ELSE EXTRACT(DOY FROM d) + (365 - EXTRACT(DOY FROM MAKE_DATE(EXTRACT(YEAR FROM d) - 1, 10, 1)) + 1)
    END AS water_year_day,

    -- Hydrology: Meteorological Seasons
    CASE 
        WHEN EXTRACT(MONTH FROM d) IN (12, 1, 2) THEN 'Winter'
        WHEN EXTRACT(MONTH FROM d) IN (3, 4, 5) THEN 'Spring'
        WHEN EXTRACT(MONTH FROM d) IN (6, 7, 8) THEN 'Summer'
        WHEN EXTRACT(MONTH FROM d) IN (9, 10, 11) THEN 'Fall'
    END AS season,
    
    -- Custom Hydrology Groupings (e.g., MN typical recharge/runoff seasons)
    CASE 
        WHEN EXTRACT(MONTH FROM d) IN (10, 11, 12, 1, 2, 3) THEN TRUE 
        ELSE FALSE 
    END AS is_recharge_season

FROM date_series;