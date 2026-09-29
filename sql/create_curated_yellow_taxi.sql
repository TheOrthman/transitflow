CREATE EXTERNAL TABLE IF NOT EXISTS transitflow.curated_yellow_taxi (
    vendor_id INT,
    pickup_datetime TIMESTAMP,
    dropoff_datetime TIMESTAMP,
    passenger_count BIGINT,
    trip_distance DOUBLE,
    rate_code_id BIGINT,
    store_and_forward_flag STRING,
    pickup_location_id INT,
    dropoff_location_id INT,
    payment_type BIGINT,
    fare_amount DOUBLE,
    extra DOUBLE,
    mta_tax DOUBLE,
    tip_amount DOUBLE,
    tolls_amount DOUBLE,
    improvement_surcharge DOUBLE,
    total_amount DOUBLE,
    congestion_surcharge DOUBLE,
    airport_fee DOUBLE,
    cbd_congestion_fee DOUBLE,
    quarantine_reasons ARRAY<STRING>,
    warning_reasons ARRAY<STRING>,
    validation_status STRING,
    source_file STRING,
    batch_id STRING,
    schema_version STRING,
    ingested_at STRING
)
PARTITIONED BY (
    year INT,
    month INT
)
STORED AS PARQUET
LOCATION 's3://transitflow-data-1789925359/curated/yellow_taxi/';
