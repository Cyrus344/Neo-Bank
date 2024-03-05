with 

source as (

    select * from {{ source('cleaned', 'transactions') }}

),

renamed as (

    select
        transaction_id,
        transactions_type,
        transactions_currency,
        amount_usd,
        transactions_state,
        holder_presence,
        merchant_mcc,
        merchant_city,
        merchant_country,
        direction,
        user_id,
        created_date

    from source

)

select * from renamed
