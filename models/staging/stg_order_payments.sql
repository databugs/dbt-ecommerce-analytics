{{ config(materialized='view') }}

WITH source AS (
    SELECT * 
    FROM {{ source('raw_data', 'order_payments') }}
)
select
    order_id,
    payment_sequential,
    payment_type,
    payment_installments,
    payment_value
from source