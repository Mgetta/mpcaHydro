

CREATE TABLE IF NOT EXISTS facts.continuous (
    station_sk INTEGER REFERENCES dimensions.station(station_sk),
    constituent_sk INTEGER REFERENCES dimensions.constituent(constituent_sk),
    statistic_sk INTEGER REFERENCES dimensions.statistic(statistic_sk), 
    quality_code VARCHAR,             
    quality_code_name VARCHAR,             
    
    reading_datetime TIMESTAMP, 
    reading_date DATE,                   
    reading_time TIME,                   
    interval_minutes INTEGER,    
    value FLOAT
);


-- Chemistry Fact Table (Discrete grab samples)
CREATE TABLE IF NOT EXISTS facts.discrete (
    station_sk INTEGER REFERENCES dimensions.station(station_sk),
    constituent_sk INTEGER REFERENCES dimensions.constituent(constituent_sk),
    statistic_sk INTEGER REFERENCES dimensions.statistic(statistic_sk), 
    
    -- Changed to VARCHAR and removed the foreign key constraint
    quality_code VARCHAR,
    quality_code_name VARCHAR,             
    sample_datetime TIMESTAMP,      
    value FLOAT,
    result_text VARCHAR,              
    sample_method VARCHAR,              
    analysis_method VARCHAR,            
    lab_qualifier VARCHAR,              
    sample_date DATE,                    
    sample_time TIME                     
);
