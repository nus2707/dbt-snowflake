with listings as (
    select * from {{ ref('stg_listings') }}
),
hosts as (
    select * from {{ ref('stg_hosts') }}
)
select
    l.listing_id,
    l.property_type,
    l.room_type,
    l.city,
    l.country,
    l.price_per_night,
    h.host_id,
    h.host_name,
    h.is_superhost,
    h.response_rate
from listings l
join hosts h on l.host_id = h.host_id
