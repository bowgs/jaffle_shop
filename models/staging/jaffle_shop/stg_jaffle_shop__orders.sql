-- select
--     id as order_id,
--     user_id as customer_id,
--     order_date,
--     status
-- from {{source ('jaffle_shop', 'orders') }}
WITH source AS (
    SELECT
        *
    FROM
        {{ source (
            'jaffle_shop',
            'orders'
        ) }}
),
transformed AS (
    SELECT
        id AS order_id,
        user_id AS customer_id,
        order_date,
        status
    FROM
        source
)
SELECT
    *
FROM
    transformed
