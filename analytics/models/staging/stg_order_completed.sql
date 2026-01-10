select
    order_id,
    status,
    cast(completed_at as timestamp) as completed_at
from {{ source('raw', 'order_completed') }}