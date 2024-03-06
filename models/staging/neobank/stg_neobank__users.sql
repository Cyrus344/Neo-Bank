with 

source as (

    select * from {{ source('neobank', 'users') }}

),

renamed as (

    select
        user_id,
        birth_year,
        country,
        city,
        created_date,
        user_settings_crypto_unlocked,
        plan,
        attributes_notifications_marketing_push,
        attributes_notifications_marketing_email,
        num_contacts,
        num_referrals,
        num_successful_referrals

    from source

)

select * from renamed
