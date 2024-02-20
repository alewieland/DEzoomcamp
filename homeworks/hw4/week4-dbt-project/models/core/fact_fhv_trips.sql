{{
    config(
        materialized='table'
    )
}}

with fhv_tripdata as (
    select *
    from {{ ref('stg_fhv_tripdata') }}
),
dim_zones as (
    select * 
    from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)
select 
    
    fhv_tripdata.tripid, 
    fhv_tripdata.dispatching_base_num,
    fhv_tripdata.pickup_datetime, 
    fhv_tripdata.dropoff_datetime, 
    fhv_tripdata.pickup_locationid, 
    fhv_tripdata.dropoff_locationid, 
    fhv_tripdata.sharedride_flag, 
    fhv_tripdata.affiliated_base_number, 
    pickup_zones.borough as pickup_borough, 
    pickup_zones.zone as pickup_zone, 
    dropoff_zones.borough as dropoff_borough, 
    dropoff_zones.zone as dropoff_zone  
    
    --count(*)
from fhv_tripdata
inner join dim_zones as pickup_zones
on fhv_tripdata.pickup_locationid = pickup_zones.locationid
inner join dim_zones as dropoff_zones
on fhv_tripdata.dropoff_locationid = dropoff_zones.locationid