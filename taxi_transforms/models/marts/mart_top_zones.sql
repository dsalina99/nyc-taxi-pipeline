select
    pickup_zone_id,

    count(*)                                            as trip_count,
    round(sum(fare_usd), 2)                             as total_revenue,
    round(avg(fare_usd), 2)                             as avg_fare,
    round(avg(trip_distance), 2)                        as avg_distance_miles,
    round(
        avg(tip_usd / nullif(fare_usd, 0)) * 100
    , 1)                                                as avg_tip_pct,
    round(avg(
        date_diff('minute', pickup_datetime, dropoff_datetime)
    ), 1)                                               as avg_duration_mins

from {{ ref('stg_taxi_trips') }}
group by pickup_zone_id
order by trip_count desc