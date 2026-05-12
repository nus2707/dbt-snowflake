with enriched as (
    select * from {{ ref('int_bookings_enriched') }}
)
select
    is_superhost,
    count(booking_id) as total_bookings,
    sum({{ revenue_calc('booking_amount','cleaning_fee','service_fee') }}) as total_revenue,
    avg(response_rate) as avg_response_rate,
    avg(nights_booked) as avg_nights_booked
from enriched
where booking_status = 'confirmed'
group by is_superhost
