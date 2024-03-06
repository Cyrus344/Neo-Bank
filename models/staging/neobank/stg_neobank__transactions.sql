with 

source as (

    select * from {{ source('neobank', 'transactions') }}

),

renamed as (

    select
        transaction_id,
        transactions_type as t_type,
        transactions_currency as t_currency,
        amount_usd,
        transactions_state as t_state,
        ea_cardholderpresence,
        ea_merchant_mcc as mcc,
        ea_merchant_city as merch_city,
        ea_merchant_country as merch_country,
        direction,
        user_id,
        DATETIME(created_date) as created_date

    from source

)

select * from renamed
