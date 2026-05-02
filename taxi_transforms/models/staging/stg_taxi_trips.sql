select
    ride_id,
    cast(REPLACE(tpep_pickup_datetime, 'T', ' ') as timestamp)                        as pickup_datetime,
    cast(REPLACE(tpep_dropoff_datetime, 'T', ' ') as timestamp)                       as dropoff_datetime,
    cast(trip_distance as double)               as trip_distance,
    cast(fare_amount as double)                 as fare_usd,
    cast(tip_amount as double)                  as tip_usd,
    pulocationid                                as pickup_zone_id,
    payment_type
from {{ source('nyc_taxi_db', 'trips') }}
where fare_amount > 0
  and trip_distance > 0