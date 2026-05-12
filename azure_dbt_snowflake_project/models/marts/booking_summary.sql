with enriched as (
    select * from {{ ref('int_bookings_enriched') }}
)
select
    city,
    country,
    count(booking_id) as total_bookings,
    sum(booking_amount + cleaning_fee + service_fee) as total_revenue,
    avg(booking_amount) as avg_booking_amount,
    avg(nights_booked) as avg_nights
from enriched
where booking_status = 'confirmed'
group by city, country
