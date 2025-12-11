select
    c.customer_id,
    c.customer_name,
    c.segment,
    c.country,
    c.created_at
from {{ ref('stg_customers') }} c