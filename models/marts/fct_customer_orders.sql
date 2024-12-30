with

-- import CTEs
customers as (
    select * from {{ ref('stg_jaffle_shop__customers') }}
),

orders as (
    select * from {{ ref('stg_jaffle_shop__orders') }}
),

payments as (
    select * from {{ ref('stg_stripe__payments') }}
),


-- logical CTEs

completed_payments as (
    select
        order_id,
        max(CREATED_at) as payment_finalized_date,
        sum(AMOUNT)  as total_amount_paid
    from
        payments
    where
        STATUS <> 'fail'
    group by
        1
),

-- intermediate CTES
paid_orders as (
        select
            orders.order_id,
            orders.customer_id,
            Orders.ORDER_DATE AS order_placed_at,
            Orders.STATUS AS order_status,
            completed_payments.total_amount_paid,
            completed_payments.payment_finalized_date,
            customers.FIRST_NAME as customer_first_name,
            customers.LAST_NAME as customer_last_name
            
        FROM
            orders
            -- p could be a completed payments intermediate CTE
            left join completed_payments ON orders.order_ID = completed_payments.order_id
            left join customers on orders.customer_id = customers.customer_ID
    ),

customer_orders as (
        select
            c.customer_id,
            min(ORDER_DATE) as first_order_date,
            max(ORDER_DATE) as most_recent_order_date,
            count(ORDERS.order_ID) AS number_of_orders
        from
            customers as C
            left join  orders on orders.customer_id = C.customer_ID
        group by
            1
    )  ,  

-- final CTEs
final as (

        select
                paid_orders.order_id,
                paid_orders.customer_id,
                paid_orders.order_placed_at,
                paid_orders.order_status,
                paid_orders.total_amount_paid,
                paid_orders.payment_finalized_date,
                paid_orders.customer_first_name,
                paid_orders.customer_last_name,
                ROW_NUMBER() OVER (
                                ORDER BY
                                    paid_orders.order_id
                            ) as transaction_seq,
                ROW_NUMBER() OVER (
                                PARTITION BY
                                    paid_orders.customer_id
                                ORDER BY
                                    paid_orders.order_id
                            ) as customer_sales_seq,
                CASE
                    WHEN customer_orders.first_order_date = paid_orders.order_placed_at 
                    THEN 'new'
                    ELSE 'return'
                END as nvsr,
                sum(paid_orders.total_amount_paid) over(
                                                    partition by paid_orders.customer_id 
                                                    order by paid_orders.order_placed_at
                                                    ) as customer_lifetime_value,
                customer_orders.first_order_date as fdos
        FROM
            paid_orders 
        left join customer_orders USING (customer_id)
        ORDER BY
            paid_orders.order_id

)    

select * from final