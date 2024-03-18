# Risingwave Homework

# Question 1:

-- Create Materialized view for the time of each trip for each pair of zones

CREATE MATERIALIZED VIEW trip_time AS
SELECT concat(taxi_zone.Zone, '   -   ', taxi_zone_1.Zone) as zones_pair, (tpep_dropoff_datetime - tpep_pickup_datetime) as trip_time
 FROM trip_data
 JOIN taxi_zone ON trip_data.PULocationID = taxi_zone.location_id
 JOIN taxi_zone as taxi_zone_1 ON trip_data.DOLocationID = taxi_zone_1.location_id;
 
-- Create a materialized view on top of the previous materialized view to get average min and max for each pair of zones

CREATE MATERIALIZED VIEW min_max_trip AS
with avg_trip_time as(SELECT zones_pair, AVG(trip_time) as avg_trip_time
 FROM trip_time
 GROUP BY 1),
min_trip_time_avg as (select min(avg_trip_time) as min from avg_trip_time where avg_trip_time > '00:00:00'),
max_trip_time_avg as (select max(avg_trip_time) as max from avg_trip_time)
select zones_pair, avg_trip_time from avg_trip_time, max_trip_time_avg
where avg_trip_time = max
union
select zones_pair, avg_trip_time from avg_trip_time, min_trip_time_avg
where avg_trip_time = min;

Result:

            zones_pair             | avg_trip_time 
-----------------------------------+---------------
 Washington Heights South   -   NV | 00:00:01
 Yorkville East   -   Steinway     | 23:59:33
(2 rows)



# Question 2:

WITH max_pair as (SELECT zones_pair as max_pair  FROM min_max_trip order by avg_trip_time DESC LIMIT 1)
SELECT  count(1) as trips_count from trip_time, max_pair WHERE zones_pair = max_pair;

trips_count 
-------------
           1
(1 row)


# Question 3:

with pick_up as(
SELECT
        zone, tpep_pickup_datetime
    FROM
        trip_data
            JOIN taxi_zone
                 ON trip_data.PULocationID = taxi_zone.location_id)
,last_pick_up as (SELECT max(tpep_pickup_datetime) as max FROM pick_up)
SELECT  zone, count(1) as pick_ups FROM  pick_up, last_pick_up 
WHERE tpep_pickup_datetime >= max - interval '17 hour'
GROUP BY 1
ORDER BY 2 DESC
LIMIT 3;

        zone         | pick_ups 
---------------------+----------
 LaGuardia Airport   |       19
 JFK Airport         |       17
 Lincoln Square East |       17
(3 rows)