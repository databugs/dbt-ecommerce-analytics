WITH customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),
customer_orders AS (
    SELECT * FROM {{ ref('stg_customers_orders') }}
)
SELECT
    c.customer_id,
    c.customer_unique_id,
    c.customer_zip_code_prefix,
    c.customer_city,
    c.customer_state,
    co.total_orders,
    co.first_order_date,
    co.last_order_date
FROM customers c
LEFT JOIN customer_orders co ON c.customer_id = co.customer_id;