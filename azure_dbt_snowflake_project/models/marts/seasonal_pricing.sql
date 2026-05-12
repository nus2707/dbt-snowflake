with enriched as (
    select * from {{ ref('int_bookings_enriched') }}
)
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
    avg(price_per_night) as avg_price_per_night,
    avg(booking_amount) as avg_booking_amount
from enriched
where booking_status = 'confirmed'
group by city, country, booking_month, season
order by booking_month
