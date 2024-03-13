SELECT *
FROM {{ ref('int_users_cleaned') }}
INNER JOIN {{ ref('int_user_transaction') }}
USING(user_id)