with source as (
    select * from {{ source('staging', 'hosts') }}
),
renamed as (
    select
        host_id,
        trim(host_name) as host_name,
        cast(host_since as date) as host_since,
        is_superhost,
        coalesce(response_rate, 0) as response_rate,
        cast(created_at as timestamp) as created_at
    from source
)
select * from renamed
