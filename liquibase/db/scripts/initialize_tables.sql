DROP DATABASE IF EXISTS spark_warehouse;
CREATE DATABASE spark_warehouse;

CREATE TABLE spark_warehouse.taxi_zones(
                           LocationID: int8,
                           Borough: varchar(255),
                           Zone: varchar(255),
                           service_zone: varchar(255)
);

CREATE TABLE spark_warehouse.taxi_rides(
                           VendorID: int8,
                           tpep_pickup_datetime: varchar(255),
                           tpep_dropoff_datetime: varchar(255),
                           passenger_count: int8,
                           trip_distance: float8,
                           RatecodeID: int8,
                           store_and_fwd_flag: varchar(255),
                           PULocationID: int8,
                           DOLocationID: int8,
                           payment_type: int8,
                           fare_amount: float8,
                           extra: float8,
                           mta_tax: float8,
                           tip_amount: float8,
                           tolls_amount: float8,
                           improvement_surcharge: float8,
                           total_amount: float8
);