{{ config(materialized='table') }}

with source as (
    select /* Surrogate Key */ 
    {{ dbt_utils.generate_surrogate_key(['laureate_id']) }} as nobel_laureates_key
    ,laureate_id
    ,laureate_full_name
    ,laureate_current_country
    ,laureate_award_year
    ,dbt_updated_at
    ,dbt_updated_by
    from {{ ref('brz_nobel_laureates') }}
)

SELECT * FROM source

