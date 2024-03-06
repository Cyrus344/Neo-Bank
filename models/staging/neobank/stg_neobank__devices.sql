with 

source as (

    select * from {{ source('neobank', 'devices') }}

),

renamed as (

    select
        brand,
        user_id

    from source
    where brand = "Apple" OR brand = "Android" ;

)

select * from renamed
