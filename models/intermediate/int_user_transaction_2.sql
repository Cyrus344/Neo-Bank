SELECT *
FROM {{ ref('int_users_cleaned') }}
LEFT JOIN {{ ref('int_user_transaction') }}
USING(user_id)