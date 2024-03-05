with 

source as (

    select * from {{ source('cleaned', 'devices') }}

),

renamed as (

    select
        brand,
        user_id

    from source

)

select * from renamed
