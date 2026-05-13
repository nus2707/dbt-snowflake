-- models/marts/mart_monthly_trends.sql
--
-- ============================================================
-- CONCEPT: Incremental materialisation
-- ============================================================
-- Normal dbt run:   DROP + CREATE the table from scratch every time.
--                   Fine for small tables. Expensive when you have
--                   millions of booking rows arriving daily.
--
-- Incremental run:  On the FIRST run → builds the full table.
--                   On SUBSEQUENT runs → only processes NEW rows
--                   (rows where booking_date > the max date already
--                   in the table), then MERGES them in.
--
-- Result: instead of scanning 3 years of history every morning,
-- dbt only scans yesterday's bookings. 100x faster at scale.
--
-- Grain: one row per (year, month, city, room_type, booking_status)
-- ============================================================

-- ============================================================
-- CONCEPT: config() block
-- This Jinja block sets model-level dbt settings.
-- It must be at the TOP of the file before any SQL.
--
--   materialized='incremental'
--       → tells dbt to use INSERT (new rows) not DROP+CREATE
--
--   unique_key='month_city_roomtype_status_sk'
--       → the column that uniquely identifies each row.
--         On re-runs dbt uses this to MERGE (upsert):
--         if the key already exists → UPDATE the row
--         if the key is new        → INSERT the row
--         This makes re-processing idempotent (safe to re-run).
--
--   incremental_strategy='merge'
--       → Snowflake default. Runs a MERGE statement.
--         Other options: 'delete+insert', 'append' (append-only, no dedup).
--
--   on_schema_change='fail'
--       → If you add/remove columns from this model, dbt will
--         FAIL loudly instead of silently ignoring the change.
--         Options: 'ignore' | 'append_new_columns' | 'fail' | 'sync_all_columns'
--
--   cluster_by=['booking_year', 'booking_month']
--       → Snowflake micro-partition clustering. Queries that filter
--         by year/month skip irrelevant partitions automatically.
--         Great for "show me last 3 months" dashboard queries.
-- ============================================================

{{ config(
    materialized='incremental',
    unique_key='month_city_roomtype_status_sk',
    incremental_strategy='merge',
    on_schema_change='fail',
    cluster_by=['booking_year', 'booking_month']
) }}

-- ============================================================
-- CONCEPT: is_incremental() macro
-- This built-in dbt macro returns:
--   TRUE  → when running in incremental mode (table already exists)
--   FALSE → on the first run (full refresh / table doesn't exist yet)
--
-- You wrap your date filter inside is_incremental() so that:
--   - First run: no WHERE clause → processes ALL historical data
--   - Subsequent runs: WHERE clause → only processes new months
--
-- The lookback window (-3 months) is intentional:
--   Monthly aggregates for the CURRENT month change every day
--   as new bookings arrive. We need to reprocess the last few
--   months to keep partial-month rows accurate.
-- ============================================================

with source as (

    select * from {{ ref('int_bookings_enriched') }}

    {% if is_incremental() %}

        -- On incremental runs: only load bookings from recent months.
        -- We go back 3 months (not just 1) because:
        --   - The current month's aggregate is still growing
        --   - Late-arriving bookings can back-fill previous months
        --   - Cancellations may update older months' confirmed counts
        where booking_date >= (
            select dateadd(month, -3, max(last_booking_date))
            from {{ this }}
            -- CONCEPT: 'this' keyword
            -- 'this' refers to the TARGET table itself
            -- (AIRBNB_DB.GOLD.MART_MONTHLY_TRENDS).
            -- We query the existing table to find the latest date
            -- already processed, then subtract 3 months as our
            -- safe lookback window.
        )

    {% endif %}

),

-- Pre-aggregate: compute all metrics at booking level before grouping.
-- Separating derivations from aggregations makes the GROUP BY cleaner.
monthly_aggregated as (

    select

        -- ── Time dimensions ───────────────────────────────────────
        year(booking_date)                              as booking_year,
        month(booking_date)                             as booking_month,

        -- Human-readable period label for BI tools (e.g. "2024-03")
        to_varchar(booking_date, 'YYYY-MM')             as year_month,

        -- First day of the month → useful for time-series charts
        date_trunc('month', booking_date)               as month_start_date,

        -- ── Segmentation dimensions ───────────────────────────────
        city,
        country,
        room_type,
        booking_status,

        -- ── Booking metrics ───────────────────────────────────────
        count(booking_id)                               as total_bookings,
        sum(nights_booked)                              as total_nights_booked,

        -- ── Revenue metrics ───────────────────────────────────────
        sum(booking_amount)                             as total_booking_amount,
        sum(cleaning_fee)                               as total_cleaning_fees,
        sum(service_fee)                                as total_service_fees,
        sum(booking_amount + cleaning_fee + service_fee) as total_revenue,

        avg(booking_amount + cleaning_fee + service_fee) as avg_booking_value,

        round(
            sum(booking_amount + cleaning_fee + service_fee)
            / nullif(sum(nights_booked), 0),
            2
        )                                               as avg_revenue_per_night,

        -- ── Host quality metrics ──────────────────────────────────
        avg(response_rate)                              as avg_response_rate,
        sum(case when is_superhost = 't' then 1 else 0 end) as superhost_bookings,

        -- ── Audit column ──────────────────────────────────────────
        -- Stored so the is_incremental() lookback query above can
        -- find the latest booking_date already in this table.
        max(booking_date)                               as last_booking_date

    from source

    group by
        year(booking_date),
        month(booking_date),
        to_varchar(booking_date, 'YYYY-MM'),
        date_trunc('month', booking_date),
        city,
        country,
        room_type,
        booking_status

),

final as (

    select

        -- ── Surrogate key ─────────────────────────────────────────
        -- CONCEPT: surrogate key for incremental unique_key
        -- dbt's MERGE statement uses this column to decide:
        --   "does this row already exist?" → UPDATE
        --   "is this a new combination?"   → INSERT
        --
        -- We hash all the GROUP BY dimensions together into one key.
        -- md5() is available in Snowflake without extra packages.
        md5(
            year_month
            || '|' || coalesce(city, 'UNKNOWN')
            || '|' || coalesce(room_type, 'UNKNOWN')
            || '|' || coalesce(booking_status, 'UNKNOWN')
        )                                               as month_city_roomtype_status_sk,

        -- ── Dimensions ────────────────────────────────────────────
        booking_year,
        booking_month,
        year_month,
        month_start_date,
        city,
        country,
        room_type,
        booking_status,

        -- ── Volume metrics ────────────────────────────────────────
        total_bookings,
        total_nights_booked,

        -- ── Revenue metrics ───────────────────────────────────────
        total_booking_amount,
        total_cleaning_fees,
        total_service_fees,
        total_revenue,
        round(avg_booking_value, 2)                     as avg_booking_value,
        avg_revenue_per_night,

        -- ── Host metrics ──────────────────────────────────────────
        round(avg_response_rate, 1)                     as avg_response_rate,
        superhost_bookings,

        -- Superhost share: what % of this month's bookings went to superhosts
        round(
            100.0 * superhost_bookings / nullif(total_bookings, 0),
            1
        )                                               as superhost_booking_pct,

        -- ── Audit ─────────────────────────────────────────────────
        last_booking_date,
        current_timestamp()                             as dbt_updated_at

    from monthly_aggregated

)

select * from final