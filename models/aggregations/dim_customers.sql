WITH customers AS (
    SELECT 
        *,
        ROW_NUMBER() OVER(PARTITION BY customer_unique_id) as row_seq 
        -- fields like inserted_at or created_at will help to select the most recent unique id
    FROM {{ ref('stg_customers') }}
),
customer_orders AS (
    SELECT
        customer_unique_id,
        round(sum(total_orders),2)               AS total_orders,
        round(sum(total_order_value),2)          AS total_order_value,
        round(sum(total_amount_spent),2)         AS total_amount_spent
    FROM {{ ref('customer_daily_metrics') }}
    GROUP BY 1
)
SELECT
    c.customer_unique_id,
    c.customer_zip_code_prefix,
    c.customer_city,
    c.customer_state,
    total_orders,
    total_order_value,
    total_amount_spent,
    round(total_order_value/nullif(total_orders, 0),2) AS avg_order_value
FROM customers c
LEFT JOIN customer_orders co
    ON c.customer_unique_id = co.customer_unique_id
WHERE row_seq = 1