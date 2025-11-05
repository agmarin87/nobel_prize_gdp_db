{{ config(materialized='table') }}

with source as (
    select 
    *
    ,{{ dbt.current_timestamp() }} as dbt_updated_at
    ,current_user() as dbt_updated_by 
    from {{ source('nobel_prize', 'sl_nobel_laureates') }}
)

SELECT * FROM source