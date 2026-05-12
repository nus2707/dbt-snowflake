with source as (
    select * from {{ source('staging', 'listings') }}
),
renamed as (
    select
        listing_id,
        host_id,
        property_type,
        room_type,
        city,
        country,
        coalesce(accommodates, 0) as accommodates,
        bedrooms,
        bathrooms,
        price_per_night,
        cast(created_at as timestamp) as created_at
    from source
)
select * from renamed
