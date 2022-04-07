with orders as(
    select * from {{ref('stg_orders')}}
),
payments as (
     select * from {{ref('stg_payments')}}
),
sum_payments as(
    select
        o.order_id,
        o.customer_id,
        sum(p.amount) as amount
    from orders o
    left join payments p 
        on o.order_id = p.order_id
    where p.status = 'success'
    group by 1,2
)
select * from sum_payments