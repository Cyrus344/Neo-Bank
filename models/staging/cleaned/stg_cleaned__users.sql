with 

source as (

    select * from {{ source('cleaned', 'users') }}

),

renamed as (

    select
        user_id,
        birth_year,
        country,
        country_name,
        city,
        crypto_unlocked,
        created_date,
        plan,
        notifications_push,
        notifications_email,
        num_contacts,
        num_referrals,
        num_successful_referrals

    from source

)

select * from renamed
