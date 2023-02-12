
# Question 1




Create external table:
```
CREATE OR REPLACE EXTERNAL TABLE `taxi-rides-ny-375620.ny_taxi.fhv`
OPTIONS (
  format = 'parquet',
  uris = ['gs://ny-taxi-trips-bocker/data/fhv/fhv_tripdata_2019-*.parquet']
);

```
Create materialized table from external table:
```
CREATE OR REPLACE TABLE `taxi-rides-ny-375620.ny_taxi.fhv`
as
SELECT * FROM taxi-rides-ny-375620.ny_taxi.external_fhv;
```

Count rows in table
```
SELECT Count(*) FROM `taxi-rides-ny-375620.ny_taxi.external_fhv`
```

Rows: 43244696


# Question 2

```
SELECT COUNT(affiliated_base_number) FROM taxi-rides-ny-375620.ny_taxi.external_fhv;

SELECT COUNT(affiliated_base_number) FROM taxi-rides-ny-375620.ny_taxi.fhv;
```

external: 0 MB
bigtable: 317.94 MB 

# Question 3

```
SELECT count(1)
FROM taxi-rides-ny-375620.ny_taxi.fhv
WHERE PUlocationID is NULL and DOlocationID is NULL;
```

Rows: 717748
	
# Question 4

Partition by pickup_datetime 
Cluster on affiliated_base_number

# Question 5

```
CREATE OR REPLACE TABLE `taxi-rides-ny-375620.ny_taxi.fhv_partition`
PARTITION BY date(pickup_datetime)
CLUSTER BY  affiliated_base_number  
AS
SELECT *
FROM taxi-rides-ny-375620.ny_taxi.fhv;

SELECT count(affiliated_base_number)
FROM taxi-rides-ny-375620.ny_taxi.fhv
WHERE DATE(pickup_datetime) BETWEEN '2019-03-01' and '2019-03-31';

```
(will process 647.87MB)

```

SELECT count(affiliated_base_number)
FROM taxi-rides-ny-375620.ny_taxi.fhv_partition
WHERE DATE(pickup_datetime) BETWEEN '2019-03-01' and '2019-03-31'

``` 
(will process 23.05MB)

# Question 6

GCP Bucket

# Question 7

False

# Question 8

Check [fhv_upload.py](week_2/fhv_upload.py)