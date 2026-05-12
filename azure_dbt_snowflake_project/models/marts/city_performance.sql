with enriched as (
    select * from {{ ref('int_bookings_enriched') }}
)
select
    city,
    country,
    avg(price_per_night) as avg_price_per_night,
    count(booking_id) as total_bookings,
    sum({{ revenue_calc('booking_amount','cleaning_fee','service_fee') }}) as total_revenue,
    avg(nights_booked) as avg_nights_booked
from enriched
where booking_status = 'confirmed'
group by city, country
