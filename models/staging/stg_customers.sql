{{ config(materialized='view') }}

WITH source AS (
    SELECT * 
    FROM {{ source('demo', 'customers') }}
)
select 
    customer_id,
	customer_unique_id,
	customer_zip_code_prefix,
	customer_city,
	customer_state 
from source