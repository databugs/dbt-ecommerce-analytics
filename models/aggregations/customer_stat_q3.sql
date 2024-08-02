WITH dow_purchase_stat AS (
    SELECT
        customer_unique_id,
        EXTRACT(DAYOFWEEK FROM order_date) AS day_of_week
    FROM {{ ref('customer_daily_metrics') }}
),
dow_day_rank AS (
    SELECT
        customer_unique_id,
        day_of_week,
        COUNT(day_of_week) AS day_count,
        RANK() OVER (PARTITION BY customer_unique_id ORDER BY COUNT(day_of_week) DESC) AS day_rank
    FROM dow_purchase_stat
    GROUP BY customer_unique_id, day_of_week
),
most_frequent_day as (
    SELECT
        customer_unique_id,
        day_of_week
    FROM dow_day_rank
    WHERE day_rank = 1
),
customer_orders AS (
    SELECT
        customer_unique_id,
        order_value,
        ROW_NUMBER() OVER (PARTITION BY customer_unique_id ORDER BY order_purchase_timestamp) AS order_sequence
    FROM {{ ref('fct_orders') }}
),
third_order_value AS (
    SELECT
        customer_unique_id,
        MAX(CASE WHEN order_sequence = 3 THEN order_value ELSE 0 END) AS third_order_value
    FROM customer_orders
    GROUP BY customer_unique_id
),
customers AS (
    SELECT *
    FROM {{ ref('dim_customers') }}
)
SELECT
    c.customer_unique_id    AS customer_id,
    c.customer_city         AS city,
    c.avg_order_value,
    c.total_orders,
    c.total_amount_spent,     
    CASE
        WHEN mfd.day_of_week = 1 THEN 'Sunday'
        WHEN mfd.day_of_week = 2 THEN 'Monday'
        WHEN mfd.day_of_week = 3 THEN 'Tuesday'
        WHEN mfd.day_of_week = 4 THEN 'Wednesday'
        WHEN mfd.day_of_week = 5 THEN 'Thursday'
        WHEN mfd.day_of_week = 6 THEN 'Friday'
        WHEN mfd.day_of_week = 7 THEN 'Saturday'
    END AS day_with_highest_purchase,
    COALESCE(tov.third_order_value, 0) AS third_order_value
FROM customers c
LEFT JOIN most_frequent_day mfd
    ON c.customer_unique_id = mfd.customer_unique_id
LEFT JOIN third_order_value tov
    ON c.customer_unique_id = tov.customer_unique_id