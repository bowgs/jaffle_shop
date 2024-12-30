-- select
--     id as customer_id,
--     first_name,
--     last_name
-- from {{source ('jaffle_shop', 'customers') }}
WITH source AS (
    SELECT
        *
    FROM
        {{ source (
            'jaffle_shop',
            'customers'
        ) }}
),
transformed AS (
    SELECT
        id AS customer_id,
        first_name,
        last_name
    FROM
        source
)
SELECT
    *
FROM
    transformed
