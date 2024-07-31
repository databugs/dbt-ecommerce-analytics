{{ config(materialized='view') }}

WITH source AS (
    SELECT * 
    FROM {{ source('raw_data', 'customers') }}
)

select *
from source_data