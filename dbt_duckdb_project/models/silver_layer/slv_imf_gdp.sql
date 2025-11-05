{{ config(materialized='table') }}

with source as (
    select /* Surrogate Key. There can be multiple countries for each indicator in the future*/ 
    {{ dbt_utils.generate_surrogate_key(['country_code', 'indicator']) }} as imf_gdp_key
    ,indicator as indicator_code
    ,country_code
    ,gdp_2025
    ,dbt_updated_at
    ,dbt_updated_by 
    from {{ ref('brz_imf_gdp') }}
)

SELECT * FROM source
