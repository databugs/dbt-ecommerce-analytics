{{ config(materialized='view') }}

WITH source AS (
    SELECT * 
    FROM {{ source('demo', 'sellers') }}
)

select
    seller_id, 
    seller_zip_code_prefix, 
    seller_city, 
    seller_state
from source