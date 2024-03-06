{{ config(materialized="table")}}

SELECT *
FROM {{ ref('user_transaction_complete') }}
JOIN {{ ref('stg_neobank__devices') }}
USING(user_id)