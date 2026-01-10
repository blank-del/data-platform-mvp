select
    order_id,
    customer_id,
    price,
    currency,
    cast(created_at as timestamp) as created_at
from {{ source('raw', 'order_created') }}