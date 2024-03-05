with 

source as (

    select * from {{ source('cleaned', 'notifications') }}

),

renamed as (

    select
        user_id,
        reason,
        channel,
        status,
        created_date

    from source

)

select * from renamed
