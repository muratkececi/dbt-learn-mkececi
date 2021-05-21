select
    id as payment_id,
    orderid as order_id,
    paymentmethod as payment_method,
    status as payment_status,
    {{ cents_to_dollars('amount') }} as amount,
    created as created_at,
    _batched_at
from {{ source('raw_stripe', 'payment') }}