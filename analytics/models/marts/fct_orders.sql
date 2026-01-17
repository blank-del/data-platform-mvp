select
    c.order_id as order_id,
    c.customer_id as customer_id,
    c.price as price,
    c.currency as currency,
    c.created_at as created_at,
    co.completed_at as completed_at,
    co.status
from {{ ref('stg_order_created') }} c
left join {{ ref('stg_order_completed') }} co
    using (order_id)