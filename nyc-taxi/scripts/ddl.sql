-- =================================================================
--  Materialize & NYC Yellow Taxi Data
-- =================================================================

-- Raw table to match the full NYC yellow taxi trip data, if useful
DROP TABLE IF EXISTS yellow_taxi_trips_direct;
CREATE TABLE yellow_taxi_trips_direct (
    VendorID                BIGINT,
    tpep_pickup_datetime    TIMESTAMP,
    tpep_dropoff_datetime   TIMESTAMP,
    passenger_count         DOUBLE PRECISION,
    trip_distance           DOUBLE PRECISION,
    RatecodeID              DOUBLE PRECISION, -- Kept as DOUBLE to match source, though often an INT
    store_and_fwd_flag      VARCHAR,
    PULocationID            BIGINT,
    DOLocationID            BIGINT,
    payment_type            BIGINT,
    fare_amount             DOUBLE PRECISION,
    extra                   DOUBLE PRECISION,
    mta_tax                 DOUBLE PRECISION,
    tip_amount              DOUBLE PRECISION,
    tolls_amount            DOUBLE PRECISION,
    improvement_surcharge   DOUBLE PRECISION,
    total_amount            DOUBLE PRECISION,
    congestion_surcharge    DOUBLE PRECISION,
    Airport_fee             DOUBLE PRECISION
);


-- For Streaming demo using Kafka:


-- Simple Kafka CONNECTION that works for us; no auth, no ssl! demo!
CREATE CONNECTION yellow_taxi_trips_kafka TO KAFKA (
    BROKER 'kafka-nyc-taxi:29092',
    SECURITY PROTOCOL = 'PLAINTEXT'
);

-- SOURCE definition
DROP SOURCE IF EXISTS materialize.public.yellow_taxi_trips_source CASCADE;
CREATE SOURCE materialize.public.yellow_taxi_trips_source
IN CLUSTER quickstart
FROM KAFKA CONNECTION materialize.public.yellow_taxi_trips_kafka (TOPIC = 'yellow-taxi-trips')
KEY FORMAT TEXT VALUE FORMAT JSON
INCLUDE TIMESTAMP AS ts
ENVELOPE NONE
EXPOSE PROGRESS AS materialize.public.yellow_taxi_trips_source_progress;

-- Create a raw view that has mapped the JSON fields to SQL columns.
CREATE OR REPLACE VIEW yellow_taxi_trips AS
SELECT
    -- The `->>` operator extracts a JSON field as text.
    -- The `::` operator casts the text to the desired SQL type.
    (data->>'VendorID')::bigint                   AS "VendorID",
    (data->>'tpep_pickup_datetime')::timestamp    AS "tpep_pickup_datetime",
    (data->>'tpep_dropoff_datetime')::timestamp   AS "tpep_dropoff_datetime",
    (data->>'passenger_count')::double precision  AS "passenger_count",
    (data->>'trip_distance')::double precision    AS "trip_distance",
    (data->>'RatecodeID')::double precision       AS "RatecodeID",
    (data->>'store_and_fwd_flag')::varchar        AS "store_and_fwd_flag",
    (data->>'PULocationID')::bigint               AS "PULocationID",
    (data->>'DOLocationID')::bigint               AS "DOLocationID",
    (data->>'payment_type')::bigint               AS "payment_type",
    (data->>'fare_amount')::double precision      AS "fare_amount",
    (data->>'extra')::double precision            AS "extra",
    (data->>'mta_tax')::double precision          AS "mta_tax",
    (data->>'tip_amount')::double precision       AS "tip_amount",
    (data->>'tolls_amount')::double precision     AS "tolls_amount",
    (data->>'improvement_surcharge')::double precision AS "improvement_surcharge",
    (data->>'total_amount')::double precision     AS "total_amount",
    (data->>'congestion_surcharge')::double precision AS "congestion_surcharge",
    (data->>'Airport_fee')::double precision      AS "Airport_fee"
FROM
    materialize.public.yellow_taxi_trips_source;

-- Create a MATERIALIZED VIEW to get a real-time count of trips
-- for the top 10 busiest pickup locations.

CREATE OR REPLACE MATERIALIZED VIEW busiest_pickup_locations AS
SELECT
    "PULocationID",
    count(*) AS trip_count
FROM
    yellow_taxi_trips
GROUP BY
    "PULocationID"
ORDER BY
    trip_count DESC
LIMIT 7;

-- Create a MATERIALIZED VIEW for a real-time display of the top tipped trips.
CREATE OR REPLACE MATERIALIZED VIEW top_tipped_trips AS
SELECT
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "fare_amount",
    "tip_amount",
    "total_amount"
FROM
    yellow_taxi_trips
WHERE
    "tip_amount" > 0
ORDER BY
    "tip_amount" DESC
LIMIT 7;

-- POPULAR TRIPS (the pickup-dropoff pairs with the most trips)
CREATE OR REPLACE MATERIALIZED VIEW popular_trips AS
SELECT
    "PULocationID" AS "pickup_location",
    "DOLocationID" AS "dropoff_location",
    count(*) AS "number_of_trips"
FROM
    yellow_taxi_trips
GROUP BY
    "PULocationID",
    "DOLocationID"
ORDER BY
    "number_of_trips" DESC
LIMIT 7;