with payments as (

    select *
    from {{ref('stg_stripe__payments') }}

),

orders as  (

    select * 
    from {{ref('stg_jaffle_shop__orders')}}

),

order_payments as (
    select
        order_id,
        sum(case when status = 'success' then amount else 0 end ) as amount
    from payments
    group by 1
),

final  as (
select 
    orders.order_id,
    orders.customer_id,
    orders.order_date,
    coalesce(order_payments.amount,0) as amount

from orders
left join order_payments using (order_Id)

)

select * from final 