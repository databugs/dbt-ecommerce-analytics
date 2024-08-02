WITH products AS (
    SELECT * FROM {{ ref('stg_products') }}
),
product_orders AS (
    SELECT
        product_id,
        sum(1) AS total_orders,
        sum(num_items) AS total_quantity,
        SUM(order_value) AS total_revenue
    FROM {{ ref('stg_order_items_agg') }}
    GROUP BY 1
)
SELECT
    p.product_id,
    p.product_category_name,
    p.product_name_lenght,
    p.product_description_lenght,
    p.product_photos_qty,
    p.product_weight_g,
    p.product_length_cm,
    p.product_height_cm,
    p.product_width_cm,
    po.total_orders,
    po.total_quantity,
    po.total_revenue
FROM products p
LEFT JOIN product_orders po ON p.product_id = po.product_id