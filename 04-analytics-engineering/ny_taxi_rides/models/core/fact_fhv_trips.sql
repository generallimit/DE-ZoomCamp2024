{{
    config(
        materialized='table'
    )
}}

with fhv_trip_data as (
    select *,
    'FHV' as service_type
    from {{ ref('stg_fhv_tripdata') }}
),
dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)

select  fhv_trip_data.tripid, 
        fhv_trip_data.dispatching_base_num, 
        fhv_trip_data.service_type, 
        fhv_trip_data.PUlocationID, 
        pickup_zone.borough as pickup_borough, 
        pickup_zone.zone as pickup_zone, 
        fhv_trip_data.DOlocationID,
        dropoff_zone.borough as dropoff_borough, 
        dropoff_zone.zone as dropoff_zone,  
        fhv_trip_data.pickup_datetime, 
        fhv_trip_data.dropOff_datetime, 
        fhv_trip_data.SR_Flag,
        fhv_trip_data.Affiliated_base_number, 
from fhv_trip_data
inner join dim_zones as pickup_zone
    on fhv_trip_data.PUlocationID = pickup_zone.locationid
inner join dim_zones as dropoff_zone
    on fhv_trip_data.DOlocationID = dropoff_zone.locationid