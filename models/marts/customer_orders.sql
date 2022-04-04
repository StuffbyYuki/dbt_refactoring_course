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

)

paid_orders as (
    
    select 
      Orders.ID         as order_id,
      Orders.USER_ID    as customer_id,
      Orders.ORDER_DATE as order_placed_at,
      Orders.status     as order_status,
      p.total_amount_paid,
      p.payment_finalized_date,
      C.FIRST_NAME      as customer_first_name,
      C.LasT_NAME       as customer_last_name
    from orders

    left join complete_payments p 
        on orders.ID = p.order_id

    left join customers C 
        on orders.USER_ID = C.ID 

),

customer_orders as (
    select 
      C.ID as customer_id,
      min(ORDER_DATE) as first_order_date,
      max(ORDER_DATE) as most_recent_order_date,
      count(ORDERS.ID) as number_of_orders
    from customers C 
    left join orders
        on orders.USER_ID = C.ID 
    group by 1
),

final as (

    select
    p.*,
    ROW_NUMBER() OVER (order by p.order_id) as transaction_seq,
    ROW_NUMBER() OVER (partition by customer_id order by p.order_id) as customer_sales_seq,
    case when c.first_order_date = p.order_placed_at
            THEN 'new'
            ELSE 'return' 
    end as nvsr,
    
    
    sum(total_amount_paid) over (partition by paid_orders.customer_id order by paid_orders.order_placed_at) as customer_lifetime_value,
    c.first_order_date as fdos
    from paid_orders p
    left join customer_orders as c using (customer_id)
    order by order_id

)

select * from final