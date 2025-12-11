
  create view "bizmvp"."marts"."dim_products__dbt_tmp"
    
    
  as (
    select
    p.product_id,
    p.product_name,
    p.category,
    p.price
from "bizmvp"."staging"."stg_products" p
  );