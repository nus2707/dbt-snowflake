-- models/marts/mart_revenue_by_host.sql
--
-- Aggregated host performance report.
-- Grain: one row per host — not per booking.
-- This is an example of a "metrics mart" — useful for dashboards like
-- "Top 10 hosts by revenue" or "Superhost vs regular host earnings".

with enriched as (

    select * from {{ ref('int_bookings_enriched') }}

)

select

    host_name,
    is_superhost,
    city,
    country,

    -- Volume metrics
    count(booking_id)                                       as total_bookings,
    count(distinct listing_id)                              as total_listings,
    sum(nights_booked)                                      as total_nights_booked,

    -- Revenue metrics (confirmed bookings only)
    sum(
        case when booking_status = 'confirmed'
             then booking_amount + cleaning_fee + service_fee
             else 0
        end
    )                                                       as confirmed_revenue,

    -- Cancellation rate (useful for host quality scoring)
    round(
        100.0 * sum(case when booking_status = 'cancelled' then 1 else 0 end)
        / nullif(count(booking_id), 0),
        1
    )                                                       as cancellation_rate_pct,

    -- Average booking value
    round(
        avg(booking_amount + cleaning_fee + service_fee),
        2
    )                                                       as avg_booking_value,

    -- Average response rate across all their listings
    round(avg(response_rate), 1)                            as avg_response_rate

from enriched

-- CONCEPT — GROUP BY
-- We're collapsing many booking rows into one row per host.
-- Every non-aggregated column in SELECT must appear in GROUP BY.
group by
    host_name,
    is_superhost,
    city,
    country

order by confirmed_revenue desc nulls last
