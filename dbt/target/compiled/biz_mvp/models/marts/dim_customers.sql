select
    c.customer_id,
    c.customer_name,
    c.segment,
    c.country,
    c.created_at
from "bizmvp"."staging"."stg_customers" c