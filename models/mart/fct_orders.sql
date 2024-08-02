WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),
order_items AS (
    SELECT 
        order_id,
        sum(num_items) AS num_items,
        sum(order_value) AS order_value,
        sum(freight_value) AS freight_value
    FROM {{ ref('stg_order_items_agg') }}
    GROUP BY 1 
),
payments AS (
    SELECT
        order_id,
        sum(payment_value) as amount_paid
    FROM {{ ref('stg_order_payments') }}
    GROUP By 1
),
customers AS (
    SELECT customer_id, customer_unique_id
    FROM {{ ref('stg_customers') }}
)
SELECT
    o.order_id,
    o.customer_id,
    c.customer_unique_id,
    o.order_status,
    o.order_purchase_timestamp,
    o.order_approved_at,
    o.order_delivered_carrier_date,
    o.order_delivered_customer_date,
    o.order_estimated_delivery_date,
    oi.num_items,
    oi.order_value,
    oi.freight_value,
    p.amount_paid
FROM orders o
LEFT JOIN order_items oi 
	ON o.order_id = oi.order_id
LEFT JOIN payments p
    ON o.order_id = p.order_id
LEFT JOIN customers c
    ON o.customer_id = c.customer_id