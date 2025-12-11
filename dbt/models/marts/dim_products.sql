select
    p.product_id,
    p.product_name,
    p.category,
    p.price
from {{ ref('stg_products') }} p