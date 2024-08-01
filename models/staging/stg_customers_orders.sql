SELECT
    customer_id,
    COUNT(order_id) AS total_orders,
    MIN(order_purchase_timestamp) AS first_order_date,
    MAX(order_purchase_timestamp) AS last_order_date
FROM {{ ref('stg_orders') }}
GROUP BY 1