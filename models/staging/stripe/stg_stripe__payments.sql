-- select 
--     id as payment_id,
--     orderid as order_id,
--     paymentmethod as payment_method,
--     status,
--     amount/100 as amount,
--     created as created_at

-- from {{source ('stripe', 'payment') }}

with 
source as (
    select * from {{source ('stripe', 'payment') }}
),
transformed as (
    select 
        id as payment_id,
        orderid as order_id,
        paymentmethod as payment_method,
        status,
        amount/100 as amount,
        created as created_at    
    from source
)

select * from transformed
