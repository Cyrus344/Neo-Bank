with 

source as (

    select * from {{ source('neobank', 'notifications') }}

),

renamed as (

    select
        reason,
        channel,
        status,
        user_id,
        CAST(created_date AS DATETIME) AS created_date
    from source

)

select * from renamed
