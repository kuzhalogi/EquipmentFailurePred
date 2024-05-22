CREATE TABLE prediction (
    id SERIAL PRIMARY KEY,
    product_id VARCHAR(255) ,
    air_temperature_k FLOAT,
    process_temperature_k FLOAT,
    rotational_speed_rpm INT,
    torque_nm FLOAT,
    tool_wear_min INT,
    type VARCHAR(255),
    prediction INT,
    date DATE,
    source VARCHAR(255)
);

CREATE TABLE data_errors (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP,
    file_number VARCHAR,
    row_index INTEGER,
    column_name VARCHAR,
    error VARCHAR,
    criticality VARCHAR
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