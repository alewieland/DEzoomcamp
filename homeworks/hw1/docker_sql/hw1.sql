-- Q3
SELECT COUNT(*) AS total_trips
FROM 
    green_tripdata
WHERE 
    CAST(lpep_pickup_datetime AS DATE) = '2019-09-18' AND 
    CAST(lpep_dropoff_datetime AS DATE) = '2019-09-18';


-- Q4
SELECT lpep_pickup_datetime, trip_distance
FROM green_tripdata
WHERE
    CAST(lpep_pickup_datetime AS DATE) IN ('2019-09-18', '2019-09-16', '2019-09-26', '2019-09-21')
ORDER BY trip_distance DESC
LIMIT 1;

-- Q5
SELECT *
FROM (
    SELECT
        CAST(t."lpep_pickup_datetime" AS DATE) as "day",
        z."Borough",
        SUM(t."total_amount") AS total_amount
    FROM
        green_tripdata t
        LEFT JOIN zone_lut z ON t."PULocationID" = z."LocationID"
    WHERE
        CAST(t."lpep_pickup_datetime" AS DATE) = '2019-09-18' AND
        z."Borough" != 'Unknown'
    GROUP BY
        z."Borough", CAST(t."lpep_pickup_datetime" AS DATE)
) AS subquery
WHERE
    total_amount > 50000
ORDER BY
    total_amount DESC;

-- Q6
SELECT
    CAST(t."lpep_pickup_datetime" AS DATE) as "day",
    zpu."Zone" as "pickup_zone",
    zdo."Zone" as "dropoff_zone",
    t."tip_amount"
FROM
    green_tripdata t
    JOIN zone_lut zpu ON t."PULocationID" = zpu."LocationID"
    JOIN zone_lut zdo ON t."DOLocationID" = zdo."LocationID"
WHERE
    zpu."Zone" = 'Astoria'
ORDER BY
    t."tip_amount" DESC
LIMIT 1;
