from dagster import AssetExecutionContext, asset, AssetKey # for setting dependencies
from dagster_dbt import DbtCliResource, dbt_assets, get_asset_key_for_model

from .project import dbt_duckdb_project_project

# Define the dbt assets, with a dependency on the ingestion_country_gdp asset
@dbt_assets(manifest=dbt_duckdb_project_project.manifest_path)
def dbt_duckdb_project_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()

# Define the ingestion_country_gdp asset
@asset(compute_kind="python")
def ingestion_imf_indicator():
    import requests
    import pandas as pd
    import os
    import duckdb

    # Fetch indicators from IMF API
    url = "https://www.imf.org/external/datamapper/api/v1/indicators"
    response = requests.get(url)

    # Extract the indicators data from the JSON response
    imf_indicator = response.json().get('indicators', {})

    # Process the indicators into a DataFrame
    imf_indicator_code = list(imf_indicator.keys())
    imf_indicator_label = [v.get("label") for v in imf_indicator.values()]
    imf_indicator_description = [v.get("description") for v in imf_indicator.values()]
    imf_indicator_unit = [v.get("unit") for v in imf_indicator.values()]

    df_imf_indicators = pd.DataFrame({'code': imf_indicator_code, 
                                'label': imf_indicator_label, 
                                'description': imf_indicator_description,
                                'unit': imf_indicator_unit})
    
    os.chdir(r"C:\Users\Antonio\my_projects\nobel_prize_gdp_db\dbt_duckdb_project")
    conn = duckdb.connect(database='./dev.duckdb', read_only=False) # Connect to DuckDB database
    conn.register('data', df_imf_indicators) # Register the DataFrame as a DuckDB table
    conn.execute("COPY data TO 'data.parquet' (FORMAT 'parquet')") # Save the DataFrame as a Parquet file
    conn.execute("CREATE TABLE IF NOT EXISTS sl_imf_indicators AS SELECT * FROM 'data.parquet'") # Create a DuckDB table from the Parquet file
    conn.close()


@asset(compute_kind="python")
def ingestion_imf_country():
    import requests
    import pandas as pd
    import os
    import duckdb

    # Fetch countries from IMF API
    url = "https://www.imf.org/external/datamapper/api/v1/countries"
    response = requests.get(url)

    # Extract the countries data from the JSON response
    imf_countries = response.json().get('countries', {})

    # Process the countries into a DataFrame
    imf_country_code = list(imf_countries.keys())
    imf_country_name = [v.get("label") for v in imf_countries.values()]

    df_imf_countries = pd.DataFrame({'code': imf_country_code, 
                                'name': imf_country_name})

    os.chdir(r"C:\Users\Antonio\my_projects\nobel_prize_gdp_db\dbt_duckdb_project")
    conn = duckdb.connect(database='./dev.duckdb', read_only=False) # Connect to DuckDB database
    conn.register('data', df_imf_countries) # Register the DataFrame as a DuckDB table
    conn.execute("COPY data TO 'data.parquet' (FORMAT 'parquet')") # Save the DataFrame as a Parquet file
    conn.execute("CREATE TABLE IF NOT EXISTS sl_imf_countries AS SELECT * FROM 'data.parquet'") # Create a DuckDB table from the Parquet file
    conn.close()

@asset(compute_kind="python")
def ingestion_imf_gdp():
    import requests
    import pandas as pd
    import os
    import duckdb

    # Fetch GDP data from IMF API
    url = "https://www.imf.org/external/datamapper/api/v1/NGDPDPC"
    response = requests.get(url)
    indicator_code = "NGDPDPC"

    # Extract the GDP data from the JSON response
    imf_gdp_data = response.json().get('values', {}).get(indicator_code, {})


    # Process the GDP data into a DataFrame
    imf_gdp_country_code = list(imf_gdp_data.keys())
    imf_gdp_2025 = [v.get("2025") for v in imf_gdp_data.values()]

    df_imf_gdp = pd.DataFrame({'country_code': imf_gdp_country_code, 
                            'gdp_2025': imf_gdp_2025})
    df_imf_gdp['indicator'] = indicator_code

    os.chdir(r"C:\Users\Antonio\my_projects\nobel_prize_gdp_db\dbt_duckdb_project")
    conn = duckdb.connect(database='./dev.duckdb', read_only=False) # Connect to DuckDB database
    conn.register('data', df_imf_gdp) # Register the DataFrame as a DuckDB table
    conn.execute("COPY data TO 'data.parquet' (FORMAT 'parquet')") # Save the DataFrame as a Parquet file
    conn.execute("CREATE TABLE IF NOT EXISTS sl_imf_gdp AS SELECT * FROM 'data.parquet'") # Create a DuckDB table from the Parquet file
    conn.close()

@asset(compute_kind="python")
def ingestion_nobelprize():
    import requests
    import pandas as pd
    import os
    import duckdb

    # Extract the total number of laureates from the JSON response
    url = "https://api.nobelprize.org/2.1/laureates"
    response = requests.get(url)
    n = response.json().get('meta', {}).get('count', 'Not available')

    # Fetch all laureates using the total count
    url = "https://api.nobelprize.org/2.1/laureates?offset=0&limit=" + str(n)
    response = requests.get(url)

    # Extract the laureates data from the JSON response
    df_nobel_laureates = pd.DataFrame()

    for i in range(n):
        nobel_laureates_id = response.json().get('laureates', {})[i].get('id', 'Unknown')
        nobel_laureates_name = response.json().get('laureates', {})[i].get('fullName', {}).get('en', 'Unknown')
        nobel_laureates_current_country = response.json().get('laureates', {})[i].get('birth', {}).get('place', {}).get('countryNow', {}).get('en', 'Unknown')
        nobel_laureates_award_year = response.json().get('laureates', {})[i].get('nobelPrizes', [{}])[0].get('awardYear', 'Unknown')

        # Process the laureates into a DataFrame
        df_nobel_laureates = pd.concat([df_nobel_laureates, 
                                    pd.DataFrame({'laureate_id': [nobel_laureates_id],
                                        'laureate_full_name': [nobel_laureates_name],
                                        'laureate_current_country': [nobel_laureates_current_country],
                                        'laureate_award_year': [nobel_laureates_award_year]})],
                                    ignore_index=True)
    
    os.chdir(r"C:\Users\Antonio\my_projects\nobel_prize_gdp_db\dbt_duckdb_project")
    conn = duckdb.connect(database='./dev.duckdb', read_only=False) # Connect to DuckDB database
    conn.register('data', df_nobel_laureates) # Register the DataFrame as a DuckDB table
    conn.execute("COPY data TO 'data.parquet' (FORMAT 'parquet')") # Save the DataFrame as a Parquet file
    conn.execute("CREATE TABLE IF NOT EXISTS sl_nobel_laureates AS SELECT * FROM 'data.parquet'") # Create a DuckDB table from the Parquet file
    conn.close()

@asset(compute_kind="python", deps=[get_asset_key_for_model([dbt_duckdb_project_dbt_assets], "gld_laureate_gdp")])
def gld_laureate_gdp_count():
    import matplotlib.pyplot as plt
    import pandas as pd
    import duckdb
    import os
    os.chdir(r"C:\Users\Antonio\my_projects\nobel_prize_gdp_db\dbt_duckdb_project")
    conn = duckdb.connect(database='./dev.duckdb', read_only=False) # Connect to DuckDB database
    query = """
        SELECT
            gld_laureate_gdp.laureate_current_country
            ,gld_laureate_gdp.laureate_current_country_code
            ,gld_laureate_gdp.laureate_count
            ,gld_laureate_gdp.gdp_2025
        FROM 
            gld_laureate_gdp
        ORDER BY gld_laureate_gdp.gdp_2025 DESC
        ;
        """
    df_laureate_gdp = conn.execute(query).df()
    conn.close()
    # Plotting
    x = df_laureate_gdp['gdp_2025']
    y = df_laureate_gdp['laureate_count']
    for i, txt in enumerate(df_laureate_gdp['laureate_current_country']):
        plt.annotate(txt, (x[i], y[i]), textcoords="offset points", xytext=(0,5), ha='center')
    plt.scatter(x, y)
    plt.title('Nobel Laureates vs GDP')
    plt.xlabel('GDP 2025')
    plt.ylabel('Number of Nobel Laureates')
    plt.grid(True)
    plt.show()