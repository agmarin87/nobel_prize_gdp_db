{{ config(materialized='table') }}

with source as (
    select /* Surrogate Key */ 
    {{ dbt_utils.generate_surrogate_key(['code']) }} as imf_indicators_key
    ,code as indicator_code
    ,label as indicator_label
    ,description as indicator_description
    ,unit as indicator_unit
    ,dbt_updated_at
    ,dbt_updated_by 
    from {{ ref('brz_imf_indicators') }}
)

SELECT * FROM source