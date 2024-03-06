SELECT *
FROM {{ ref('user_transaction_completed') }}
LEFT JOIN {{ ref('stg_neobank__devices') }}
USING(user_id)