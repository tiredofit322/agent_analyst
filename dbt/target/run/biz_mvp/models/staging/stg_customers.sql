
  create view "bizmvp"."staging"."stg_customers__dbt_tmp"
    
    
  as (
    with src as (
    select * from "bizmvp"."raw"."customers"
)
select
    cast(customer_id as int) as customer_id,
    customer_name,
    segment,
    country,
    cast(created_at as date) as created_at
from src
  );