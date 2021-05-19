with orders as (
    select * from {{ ref('stg_orders') }}
),
payments as (
    select * from {{ ref('stg_payments') }}
),
final as (
    select
        orders.order_id,
        orders.customer_id,
        sum(payments.amount) as amount,
        orders.order_date,
        orders.status as order_status
    from orders
    left join payments on orders.order_id = payments.order_id
    where payments.payment_status = 'success'
    group by 1, 2, 4, 5
)
select * from final