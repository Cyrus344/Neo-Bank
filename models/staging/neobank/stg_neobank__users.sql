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
        EXTRACT (DATETIME FROM created_date) AS created_date, 
        CAST(user_settings_crypto_unlocked AS BOOL) AS crypto_unlocked,
        INITCAP (plan) AS plan,
        CAST (attributes_notifications_marketing_push AS STRING) AS notifications_push,
        CAST (attributes_notifications_marketing_email AS STRING) AS notifications_email,
        num_contacts,
        num_referrals,
        num_successful_referrals

    from source

)

select * from renamed
