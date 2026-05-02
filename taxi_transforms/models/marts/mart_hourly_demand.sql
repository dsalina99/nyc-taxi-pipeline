select
    year(pickup_datetime)                          as year,
    month(pickup_datetime)                         as month,
    day(pickup_datetime)                           as day,
    hour(pickup_datetime)                          as pickup_hour,
    case
        when day_of_week(pickup_datetime) in (1,7)
        then true else false
    end                                                 as is_weekend,

    count(*)                                            as trip_count,
    round(sum(fare_usd), 2)                             as total_revenue,
    round(sum(tip_usd), 2)                              as total_tips,
    round(avg(fare_usd), 2)                             as avg_fare,
    round(avg(tip_usd), 2)                              as avg_tip,
    round(
        avg(tip_usd / nullif(fare_usd, 0)) * 100
    , 1)                                                as avg_tip_pct,
    round(avg(trip_distance), 2)                        as avg_distance_miles,
    round(avg(
        date_diff('minute', pickup_datetime, dropoff_datetime)
    ), 1)                                               as avg_duration_mins

from {{ ref('stg_taxi_trips') }}
group by
    year(pickup_datetime),
    month(pickup_datetime),
    day(pickup_datetime),
    hour(pickup_datetime),
    day_of_week(pickup_datetime)
order by year, month, day, pickup_hour