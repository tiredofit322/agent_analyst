with src as (
    select * from "bizmvp"."raw"."orders"
)
select
    cast(order_id as int) as order_id,
    cast(order_date as date) as order_date,
    cast(customer_id as int) as customer_id,
    cast(product_id as int) as product_id,
    cast(quantity as int) as quantity,
    channel
from src