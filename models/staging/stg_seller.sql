{{ config(materialized='view') }}

WITH source AS (
    SELECT * 
    FROM {{ source('raw_data', 'seller') }}
)

select
    seller_id, 
    seller_zip_code_prefix, 
    seller_city, 
    seller_state
from source