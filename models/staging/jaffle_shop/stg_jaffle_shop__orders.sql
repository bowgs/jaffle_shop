WITH 

source AS (
    SELECT * FROM {{ source ('jaffle_shop','orders') }}
),

transformed AS (
    SELECT
        id AS order_id,
        user_id AS customer_id,
        order_date,
        status
    FROM
        source
     {{ limit_data_in_dev('order_date',1000) }}
)

SELECT * FROM transformed
