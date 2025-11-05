{{ config(materialized='table') }}

with source as (
    select /* Surrogate Key */ 
    {{ dbt_utils.generate_surrogate_key(['code']) }} as imf_countries_key
    ,code as country_code
    ,name as country_name
    ,dbt_updated_at
    ,dbt_updated_by 
    from {{ ref('brz_slv_imf_countries') }}
)

SELECT * FROM source