-- hw3 create external table
CREATE OR REPLACE EXTERNAL TABLE `dtc-de-zoomcamp-2024.nytaxi.external_green_hw3`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://alewieland-taxi-data/green/green_tripdata_2022-*.parquet']
);

--check green trip data for hw3
SELECT * FROM dtc-de-zoomcamp-2024.nytaxi.external_green_hw3 limit 10;

--hw3 Create a non partitioned table from external table
CREATE OR REPLACE TABLE dtc-de-zoomcamp-2024.nytaxi.green_hw3_non_partitoned AS
SELECT * FROM dtc-de-zoomcamp-2024.nytaxi.external_green_hw3;

--hw3:q1 What is count of records for the 2022 Green Taxi Data?
--hw3:q1_ans 840,402

--hw3:q2 count the distinct number of PULocationIDs for the entire dataset on the external table
SELECT DISTINCT(PULocationID)
FROM dtc-de-zoomcamp-2024.nytaxi.external_green_hw3;
-- OB
--hw3:q2 count the distinct number of PULocationIDs for the entire dataset on the materialized table
SELECT DISTINCT(PULocationID)
FROM dtc-de-zoomcamp-2024.nytaxi.green_hw3_non_partitoned;
-- 6.41 MB

--hw3:q2_ans 0 MB for the External Table and 6.41MB for the Materialized Table

--hw3:q3 How many records have a fare_amount of 0?
SELECT count(*) as free_trips
FROM dtc-de-zoomcamp-2024.nytaxi.external_green_hw3
WHERE fare_amount = 0;
--hw3:q3_ans 1,622

--hw3:q4 What is the best strategy to make an optimized table in Big Query if your query will always order the results by PUlocationID and filter based --on lpep_pickup_datetime? (Create a new table with this strategy)
CREATE OR REPLACE TABLE dtc-de-zoomcamp-2024.nytaxi.green_hw3_partitioned_clustered
PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER BY PULocationID AS
SELECT * FROM dtc-de-zoomcamp-2024.nytaxi.external_green_hw3;
--hw3:q4_ans Partition by lpep_pickup_datetime Cluster on PUlocationID

--hw3:q5 Write a query to retrieve the distinct PULocationID between lpep_pickup_datetime 06/01/2022 and 06/30/2022 (inclusive)
--Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the --partitioned table you created for question 4 and note the estimated bytes processed. What are these values?
--Choose the answer which most closely matches.
SELECT DISTINCT(PULocationID)
FROM dtc-de-zoomcamp-2024.nytaxi.green_hw3_non_partitoned
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';

SELECT DISTINCT(PULocationID)
FROM dtc-de-zoomcamp-2024.nytaxi.green_hw3_partitioned_clustered
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';

--hw3:q5_ans 12.82 MB for non-partitioned table and 1.12 MB for the partitioned table

--hw3:q6 Where is the data stored in the External Table you created?
--hw3:q6_ans GCP bucket

--hw3:q7 It is best practice in Big Query to always cluster your data:
--hw3:q7_ans False