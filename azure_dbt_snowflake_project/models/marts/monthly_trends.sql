with enriched as (
    select * from {{ ref('int_bookings_enriched') }}
)
select
    date_trunc('month', booking_date) as booking_month,
    count(booking_id) as monthly_bookings,
    sum({{ revenue_calc('booking_amount','cleaning_fee','service_fee') }}) as monthly_revenue,
    avg(price_per_night) as avg_price_per_night
from enriched
where booking_status = 'confirmed'
group by booking_month
order by booking_month
