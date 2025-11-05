{{ config(materialized='view') }}

with source as (
    SELECT laureate_current_country as country_name_discrepancy
    FROM {{ ref('slv_nobel_laureates') }}
    EXCEPT
    SELECT country_name
    FROM {{ ref('slv_imf_countries') }}
)

SELECT * FROM source