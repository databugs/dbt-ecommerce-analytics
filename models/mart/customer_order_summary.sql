WITH customer_orders AS (
    SELECT
        c.customer_id,
        c.customer_city,
        f.order_id,
        f.total_order_value,
        f.order_purchase_timestamp,
        ROW_NUMBER() OVER (PARTITION BY c.customer_id ORDER BY f.order_purchase_timestamp) AS order_sequence
    FROM {{ ref('dim_customers') }} c
    JOIN {{ ref('fct_orders') }} f ON c.customer_id = f.customer_id
),
customer_metrics AS (
    SELECT
        customer_id,
        customer_city,
        AVG(total_order_value) AS avg_order_value,
        SUM(total_order_value) AS total_spend,
        COUNT(DISTINCT order_id) AS total_orders,
        MODE() WITHIN GROUP (ORDER BY EXTRACT(DAYOFWEEK FROM order_purchase_timestamp)) AS most_frequent_purchase_day
    FROM customer_orders
    GROUP BY customer_id, customer_city
),
third_order_value AS (
    SELECT
        customer_id,
        MAX(CASE WHEN order_sequence = 3 THEN total_order_value ELSE 0 END) AS third_order_value
    FROM customer_orders
    GROUP BY customer_id
)
SELECT
    cm.customer_id,
    cm.customer_city,
    ROUND(cm.avg_order_value, 2) AS avg_order_value,
    ROUND(cm.total_spend, 2) AS total_spend,
    cm.total_orders,
    CASE
        WHEN cm.most_frequent_purchase_day = 1 THEN 'Sunday'
        WHEN cm.most_frequent_purchase_day = 2 THEN 'Monday'
        WHEN cm.most_frequent_purchase_day = 3 THEN 'Tuesday'
        WHEN cm.most_frequent_purchase_day = 4 THEN 'Wednesday'
        WHEN cm.most_frequent_purchase_day = 5 THEN 'Thursday'
        WHEN cm.most_frequent_purchase_day = 6 THEN 'Friday'
        WHEN cm.most_frequent_purchase_day = 7 THEN 'Saturday'
    END AS day_with_highest_purchase,
    COALESCE(tov.third_order_value, 0) AS third_order_value
FROM customer_metrics cm
LEFT JOIN third_order_value tov ON cm.customer_id = tov.customer_id
ORDER BY cm.customer_id