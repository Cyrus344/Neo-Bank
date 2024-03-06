with 

source as (

    select * from {{ source('neobank', 'transactions') }}

),

renamed as (

    select
        transaction_id,
        transactions_type,
        transactions_currency,
        amount_usd,
        transactions_state,
        ea_cardholderpresence,
        ea_merchant_mcc,
        ea_merchant_city,
        ea_merchant_country,
        direction,
        user_id,
        created_date

    from source

)

select * from renamed
