with payments as (select 
    order_id,
   amount 
from {{ref('stg_stripe__payments') }}
)

select order_id, sum(amount) as total_amount
from payments
group by 1
having sum(amount) < 0