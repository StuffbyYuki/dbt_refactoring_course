with 

orders as (

    select *
    from {{ source('jaffle_shop', 'orders') }} 

),

customers as (

    select *
    from {{ source('jaffle_shop', 'customers') }}

),

payments as (

    select *
    from {{ source('stripe', 'payment') }}

),

complete_payments as (

    select 
      ORDERID as order_id, 
      max(CREATED) as payment_finalized_date, 
      sum(AMOUNT) / 100.0 as total_amount_paid
    from payments
    where status <> 'fail'
    group by 1

),

paid_orders as (
    
    select 
      Orders.ID                 as order_id,
      Orders.USER_ID            as customer_id,
      Orders.ORDER_DATE         as order_placed_at,
      Orders.status             as order_status,
      complete_payments.total_amount_paid,
      complete_payments.payment_finalized_date,
      customers.FIRST_NAME      as customer_first_name,
      customers.LasT_NAME       as customer_last_name
    from orders

    left join complete_payments
        on orders.ID = complete_payments.order_id
    left join customers
        on orders.USER_ID = customers.ID 

),

customer_orders as (
    select 
      customers.ID as customer_id,
      min(ORDER_DATE) as first_order_date,
      max(ORDER_DATE) as most_recent_order_date,
      count(ORDERS.ID) as number_of_orders
    from customers
    left join orders
        on orders.USER_ID = customers.ID 
    group by 1
),

final as (

    select
    paid_orders.*,
    ROW_NUMBER() OVER (order by paid_orders.order_id) as transaction_seq,
    ROW_NUMBER() OVER (partition by customer_id order by paid_orders.order_id) as customer_sales_seq,
    case when customer_orders.first_order_date = paid_orders.order_placed_at
            THEN 'new'
            ELSE 'return' 
    end as nvsr,
    sum(total_amount_paid) over (partition by paid_orders.customer_id order by paid_orders.order_placed_at) as customer_lifetime_value,
    c.first_order_date as fdos
    from paid_orders
    left join customer_orders
        on paid_orders.customer_id = customer_orders.customer_id
    order by order_id

)

select * from final