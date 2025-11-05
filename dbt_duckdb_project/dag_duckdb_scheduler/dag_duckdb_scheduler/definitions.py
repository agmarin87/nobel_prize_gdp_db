from dagster import Definitions
from dagster_dbt import DbtCliResource
from .assets import dbt_duckdb_project_dbt_assets, ingestion_imf_country, ingestion_imf_gdp, ingestion_imf_indicator, ingestion_nobelprize, gld_laureate_gdp_count
from .project import dbt_duckdb_project_project
from .schedules import schedules

defs = Definitions(
    assets=[dbt_duckdb_project_dbt_assets, ingestion_imf_country, ingestion_imf_gdp, ingestion_imf_indicator, ingestion_nobelprize, gld_laureate_gdp_count],
    schedules=schedules,
    resources={
        "dbt": DbtCliResource(project_dir=dbt_duckdb_project_project),
    },
)
