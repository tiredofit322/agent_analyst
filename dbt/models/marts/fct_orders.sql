with o as (
    select * from {{ ref('stg_orders') }}
),
prices as (
    select product_id, price from {{ ref('dim_products') }}
)
select
    o.order_id,
    o.order_date,
    o.customer_id,
    o.product_id,
    o.quantity,
    o.channel,
    p.price,
    (o.quantity * p.price) as revenue
from o
join prices p using (product_id)