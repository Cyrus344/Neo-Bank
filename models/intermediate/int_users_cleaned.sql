SELECT 
users.*,
codes_countries.name as country_name
FROM {{ ref('stg_neobank__users') }} as users
LEFT JOIN {{ ref('stg_neobank__codes_countries') }} as codes_countries
ON users.country = codes_countries.alpha_2