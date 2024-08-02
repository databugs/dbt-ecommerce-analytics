{{ config(materialized='view') }}

WITH source AS (
    SELECT * 
    FROM {{ source('demo', 'product_category') }}
)
select
     product_category_name,
     product_category_name_english
from source