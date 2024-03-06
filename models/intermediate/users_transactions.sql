SELECT *
FROM {{ ref('stg_cleaned__users') }}
LEFT JOIN {{ ref('transactions_users_int') }}
USING(user_id)