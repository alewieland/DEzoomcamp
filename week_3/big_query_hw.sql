-- Create Table from Bucket
CREATE OR REPLACE EXTERNAL TABLE `taxi-rides-ny-375620.ny_taxi.fhv`
OPTIONS (
  format = 'parquet',
  uris = ['gs://ny-taxi-trips-bocker/data/fhv/fhv_tripdata_2019-*.parquet']
);


CREATE OR REPLACE TABLE `taxi-rides-ny-375620.ny_taxi.fhv`
as
SELECT * FROM taxi-rides-ny-375620.ny_taxi.external_fhv;


-- Q1
SELECT Count(*) FROM `taxi-rides-ny-375620.ny_taxi.external_fhv`
-- 43244696

--Q2
SELECT COUNT(affiliated_base_number) FROM taxi-rides-ny-375620.ny_taxi.external_fhv;
-- 0B processed

SELECT COUNT(affiliated_base_number) FROM taxi-rides-ny-375620.ny_taxi.fhv;
--317.94 MB

--Q3
SELECT count(1)
FROM taxi-rides-ny-375620.ny_taxi.fhv
WHERE PUlocationID is NULL and DOlocationID is NULL;
-- 717748



--Q4 & Q5

CREATE OR REPLACE TABLE `taxi-rides-ny-375620.ny_taxi.fhv_partition`
PARTITION BY date(pickup_datetime)
CLUSTER BY  affiliated_base_number  
AS
SELECT *
FROM taxi-rides-ny-375620.ny_taxi.fhv;

SELECT count(affiliated_base_number)
FROM taxi-rides-ny-375620.ny_taxi.fhv
WHERE DATE(pickup_datetime) BETWEEN '2019-03-01' and '2019-03-31';

SELECT count(affiliated_base_number)
FROM taxi-rides-ny-375620.ny_taxi.fhv_partition
WHERE DATE(pickup_datetime) BETWEEN '2019-03-01' and '2019-03-31'