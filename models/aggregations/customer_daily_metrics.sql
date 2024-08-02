WITH orders AS (
    SELECT *
    FROM {{ ref('fct_orders') }}
)
SELECT
    DATE(order_purchase_timestamp)              AS order_date,
    customer_unique_id,
    sum(1)                                      AS total_orders,
    ROUND(COALESCE(sum(order_value),0),2)       AS total_order_value,
    ROUND(COALESCE(sum(freight_value),0),2)     AS total_freight_value,
    ROUND(COALESCE(sum(CASE 
            WHEN order_status not in ('unavailable', 'cancelled') 
                THEN amount_paid else 0 
            END
        ), 0), 2)                               AS total_amount_spent 
    -- an alternative would be order_status = 'delivered' (no risk of refund or cancellation) 
FROM orders
GROUP BY 1, 2