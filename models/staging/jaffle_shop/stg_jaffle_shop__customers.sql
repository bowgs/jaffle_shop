WITH 

source AS (
    SELECT * FROM {{ source ( 'jaffle_shop', 'customers' ) }}
),

transformed AS (
    SELECT
        id AS customer_id,
        first_name,
        last_name
    FROM
        source
)

SELECT * FROM transformed
