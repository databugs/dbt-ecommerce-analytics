WITH stg_order_items AS (
    SELECT *
    FROM {{ ref('stg_order_items') }}
)
select 
    order_id,
    product_id,
	sum(1) num_items,
	sum(price) AS order_value
    sum(freight_value) as freight_value
from stg_order_items
group by 1, 2