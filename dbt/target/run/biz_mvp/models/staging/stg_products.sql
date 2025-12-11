
  create view "bizmvp"."staging"."stg_products__dbt_tmp"
    
    
  as (
    with src as (
    select * from "bizmvp"."raw"."products"
)
select
    cast(product_id as int) as product_id,
    product_name,
    category,
    cast(price as numeric(12,2)) as price
from src
  );