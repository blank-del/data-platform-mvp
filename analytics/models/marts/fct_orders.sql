select
    c.order_id,
    c.customer_id,
    c.price,
    c.currency,
    c.created_at,
    co.completed_at,
    co.status
from {{ ref('stg_order_created') }} c
left join {{ ref('stg_order_completed') }} co
    using (order_id)