select
    payment_type,
    case payment_type
        when 1 then 'Credit Card'
        when 2 then 'Cash'
        when 3 then 'No Charge'
        when 4 then 'Dispute'
        when 5 then 'Unknown'
        when 6 then 'Voided Trip'
        else 'Other'
    end                                                 as payment_label,

    count(*)                                            as trip_count,
    round(sum(fare_usd), 2)                             as total_revenue,
    round(avg(fare_usd), 2)                             as avg_fare,
    round(avg(tip_usd), 2)                              as avg_tip,
    round(
        avg(tip_usd / nullif(fare_usd, 0)) * 100
    , 1)                                                as avg_tip_pct,
    round(
        count(*) * 100.0 / sum(count(*)) over ()
    , 1)                                                as pct_of_trips

from {{ ref('stg_taxi_trips') }}
group by payment_type
order by trip_count desc