{{ config(materialized='view') }}

WITH laureate_counts AS ( -- Preparing laureate counts by country with country codes
    SELECT DISTINCT 
        slv_nobel_laureates.laureate_current_country AS laureate_current_country
	    ,slv_imf_countries.country_code as laureate_current_country_code
        ,count(*) AS laureate_count
    FROM {{ ref('slv_nobel_laureates') }} slv_nobel_laureates
    LEFT JOIN {{ ref('slv_imf_countries') }} slv_imf_countries -- Joining to get country codes
        ON slv_nobel_laureates.laureate_current_country = slv_imf_countries.country_name
    WHERE 1=1
    AND slv_imf_countries.country_code IS NOT NULL
    GROUP BY slv_nobel_laureates.laureate_current_country, slv_imf_countries.country_code
    ORDER BY laureate_count DESC
)
SELECT -- Obtaining WITH statement columns along with GDP data
laureate_counts.laureate_current_country
,laureate_counts.laureate_current_country_code
,laureate_counts.laureate_count
,slv_imf_gdp.gdp_2025 as gdp_2025
FROM {{ ref('slv_imf_gdp') }} slv_imf_gdp
LEFT JOIN laureate_counts
    ON laureate_counts.laureate_current_country_code = slv_imf_gdp.country_code
WHERE 1=1
AND laureate_current_country_code is not null