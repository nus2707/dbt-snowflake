with bookings as (
    select * from {{ ref('stg_bookings') }}
),
listings_hosts as (
    select * from {{ ref('int_listings_hosts') }}
)
select
    b.booking_id,
    b.booking_date,
    b.nights_booked,
    b.booking_amount,
    b.cleaning_fee,
    b.service_fee,
    b.booking_status,
    lh.listing_id,
    lh.property_type,
    lh.room_type,
    lh.city,
    lh.country,
    lh.host_name,
    lh.is_superhost,
    lh.response_rate,
    lh.price_per_night   
from bookings b
join listings_hosts lh on b.listing_id = lh.listing_id
