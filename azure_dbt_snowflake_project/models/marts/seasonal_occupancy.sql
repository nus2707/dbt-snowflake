with enriched as (
    select * from {{ ref('int_bookings_enriched') }}
),
occupancy as (
    select
        city,
        country,
        date_trunc('month', booking_date) as booking_month,
        case 
            when extract(month from booking_date) in (12,1,2) then 'Winter'
            when extract(month from booking_date) in (3,4,5) then 'Spring'
            when extract(month from booking_date) in (6,7,8) then 'Summer'
            when extract(month from booking_date) in (9,10,11) then 'Fall'
        end as season,
        sum(nights_booked) as total_nights_booked,
        count(distinct booking_id) as total_bookings,
        avg(nights_booked) as avg_nights_per_booking
    from enriched
    where booking_status = 'confirmed'
    group by city, country, booking_month, season
)
select
    city,
    country,
    season,
    booking_month,
    total_nights_booked,
    total_bookings,
    avg_nights_per_booking,
    -- Simple forecasting: moving average of occupancy
    avg(total_nights_booked) over (
        partition by city, season
        order by booking_month
        rows between 2 preceding and current row
    ) as forecasted_occupancy
from occupancy
order by booking_month
