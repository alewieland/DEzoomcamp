{{
    config(
        materialized='view'
    )
}}

with tripdata as (

    select * 
    from {{ source('staging', 'external_fhv_tripdata_all') }}
    where dispatching_base_num is not null 
    AND EXTRACT(YEAR FROM pickup_datetime) = 2019
)
select
    
    -- identifiers
    {{ dbt_utils.generate_surrogate_key(['dispatching_base_num', 'pickup_datetime']) }} as tripid,
    dispatching_base_num,
       
    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropOff_datetime as timestamp) as dropoff_datetime,
    
    -- trip info
    {{ dbt.safe_cast("PUlocationID", api.Column.translate_type("integer")) }} as pickup_locationid,
    {{ dbt.safe_cast("DOlocationID", api.Column.translate_type("integer")) }} as dropoff_locationid,
    {{ dbt.safe_cast("SR_Flag", api.Column.translate_type("integer")) }} as sharedride_flag,

    -- payment info
    Affiliated_base_number as affiliated_base_number
    
    --count(*)
from tripdata
--where rn = 1

-- dbt build --select stg_fhv_tripdata --vars '{'is_test_run': 'false'}'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}