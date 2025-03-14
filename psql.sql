CREATE TABLE prediction (
    id SERIAL PRIMARY KEY,  
    product_id VARCHAR,  
    air_temperature_k DOUBLE PRECISION,  
    process_temperature_k DOUBLE PRECISION,  
    rotational_speed_rpm INTEGER,  
    torque_nm DOUBLE PRECISION,  
    tool_wear_min INTEGER,  
    type VARCHAR,
    machine_failure INTEGER,
    twf BOOLEAN DEFAULT FALSE,
    hdf BOOLEAN DEFAULT FALSE,
    pwf BOOLEAN DEFAULT FALSE,
    osf BOOLEAN DEFAULT FALSE,
    rnf BOOLEAN DEFAULT FALSE,
    failure_probability DOUBLE PRECISION,  
    date TIMESTAMP,         
    source VARCHAR
);

CREATE TABLE validation_statistics (
    file_name VARCHAR(255) PRIMARY KEY,  -- Name of the file being validated (primary key)
    total_rows INTEGER NOT NULL,  -- Total number of rows in the file
    good_rows INTEGER NOT NULL,  -- Number of rows with no validation errors
    bad_rows INTEGER NOT NULL,  -- Number of rows with at least one validation error
    failure_rate FLOAT NOT NULL,  -- Percentage of rows with validation errors
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP  -- Timestamp of when the statistics were saved
);

CREATE TABLE failed_expectations (
    id SERIAL PRIMARY KEY,  -- Auto-incrementing primary key
    file_name VARCHAR(255) REFERENCES validation_statistics(file_name),  -- Foreign key to validation_statistics
    expectation_type VARCHAR(255) NOT NULL,  -- Type of failed expectation
    failure_count INTEGER NOT NULL  -- Number of times the expectation failed
);

CREATE TABLE failed_columns (
    id SERIAL PRIMARY KEY,  -- Auto-incrementing primary key
    file_name VARCHAR(255) REFERENCES validation_statistics(file_name),  -- Foreign key to validation_statistics
    column_name VARCHAR(255) NOT NULL,  -- Name of the column with failures
    failure_count INTEGER NOT NULL  -- Number of failures in the column
);

CREATE TABLE probability (
    id SERIAL PRIMARY KEY,
    prediction_id INTEGER REFERENCES prediction(id) ON DELETE CASCADE,
    machine_failure DOUBLE PRECISION,  
    twf DOUBLE PRECISION,  
    hdf DOUBLE PRECISION,  
    pwf DOUBLE PRECISION,  
    osf DOUBLE PRECISION,  
    rnf DOUBLE PRECISION,  
    date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);



INSERT INTO prediction (
    product_id,
    air_temperature_k,
    process_temperature_k,  
    rotational_speed_rpm,
    torque_nm,
    tool_wear_min,
    type,
    prediction,
    date,
    source
) VALUES (
    'L12345',
    1230,
    2342,
    9834,
    234,
    243,
    'L',
    1,
    '2024-04-02',
    'webapp'
);