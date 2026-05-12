with source as (
    select * from {{ source('staging', 'bookings') }}
),
renamed as (
    select
        booking_id,
        listing_id,
        cast(booking_date as date) as booking_date,
        nights_booked,
        booking_amount,
        cleaning_fee,
        service_fee,
        booking_status,
        cast(created_at as timestamp) as created_at
    from source
)
select * from renamed
