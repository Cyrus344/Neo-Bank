WITH michel AS (
    SELECT
        user_id,
        EXTRACT(YEAR FROM created_date) AS year,
        EXTRACT(MONTH FROM created_date) AS month
    FROM {{ ref('stg_neobank__users') }}
)
,jakadi AS (
    SELECT
        user_id,
        CONCAT(CAST(year AS STRING), "-", CAST(month AS STRING)) AS cohort
    FROM michel
)
SELECT *
FROM jakadi
JOIN {{ ref('user_balance') }}
USING(user_id)
