-- models/marts/mart_bookings_overview.sql
--
-- CONCEPT — Mart (Gold layer)
-- Marts are the final output. BI tools (Power BI, Tableau, Metabase)
-- query these directly. Goals:
--   1. Wide: all useful columns in one place — no joins needed by analysts
--   2. Aggregated: derive business metrics that would be repetitive in SQL
--   3. Named clearly: columns should be self-explanatory to non-engineers
--
-- Materialized as TABLE (set in dbt_project.yml) so BI queries are fast.

with enriched as (

    -- Single ref() — all the hard work is already done upstream
    select * from {{ ref('int_bookings_enriched') }}

)

select

    -- ── Identifiers ───────────────────────────────────────────────
    booking_id,
    listing_id,

    -- ── Dates & time ──────────────────────────────────────────────
    booking_date,
    year(booking_date)                                  as booking_year,
    month(booking_date)                                 as booking_month,
    monthname(booking_date)                             as booking_month_name,
    dayname(booking_date)                               as booking_day_of_week,
    nights_booked,

    -- ── Location ──────────────────────────────────────────────────
    city,
    country,
    property_type,
    room_type,

    -- ── Host ──────────────────────────────────────────────────────
    host_name,
    is_superhost,
    response_rate,

    -- ── Revenue ───────────────────────────────────────────────────
    price_per_night                                     as listed_price_per_night,
    booking_amount,
    cleaning_fee,
    service_fee,

    -- Total revenue = all three fee components combined
    (booking_amount + cleaning_fee + service_fee)       as total_revenue,

    -- Revenue per night: useful for comparing bookings of different lengths
    round(
        (booking_amount + cleaning_fee + service_fee)
        / nullif(nights_booked, 0),
        2
    )                                                   as revenue_per_night,

    -- ── Status flags ─────────────────────────────────────────────
    booking_status,
    case when booking_status = 'confirmed'  then 1 else 0 end   as is_confirmed,
    case when booking_status = 'cancelled'  then 1 else 0 end   as is_cancelled,
    case when booking_status = 'pending'    then 1 else 0 end   as is_pending,

    -- ── Pricing tier (derived) ────────────────────────────────────
    case
        when price_per_night < 50   then 'Budget'
        when price_per_night < 150  then 'Mid-range'
        when price_per_night < 300  then 'Premium'
        else                             'Luxury'
    end                                                 as price_tier

from enriched
