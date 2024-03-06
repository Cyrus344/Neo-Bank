with 

source as (

    select * from {{ source('neobank', 'codes_countries') }}

),

renamed as (

    select
        name,
        alpha_2,
        alpha_3,
        country_code,
        iso_3166_2,
        region,
        sub_region,
        intermediate_region,
        region_code,
        sub_region_code,
        intermediate_region_code

    from source

)

select * from renamed
