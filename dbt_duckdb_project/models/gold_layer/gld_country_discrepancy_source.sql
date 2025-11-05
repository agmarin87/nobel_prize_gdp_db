{{ config(materialized='view') }}

with source as (
    SELECT laureate_current_country as country_name_discrepancy
    FROM {{ ref('brz_nobel_laureates') }}
    EXCEPT
    SELECT name
    FROM {{ ref('brz_imf_countries') }}
)

SELECT * FROM source