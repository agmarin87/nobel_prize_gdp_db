{{ config(materialized='table') }}

with source as (
    SELECT 
    *
    from {{ ref('brz_imf_countries') }}
),

updates as (
    SELECT
    code
    ,name
    from {{ ref('country_code_corrected') }}
),

merged as (
    SELECT 
    source.code as code
    ,coalesce(updates.name, source.name) as name
    ,source.dbt_updated_at as dbt_updated_at
    ,source.dbt_updated_by as dbt_updated_by
    from source
    left join updates
        on source.code = updates.code
)

SELECT * FROM merged