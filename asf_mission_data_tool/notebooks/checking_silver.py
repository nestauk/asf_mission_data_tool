# ---
# jupyter:
#   jupytext:
#     cell_metadata_filter: -all
#     comment_magics: true
#     custom_cell_magics: kql
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.11.2
#   kernelspec:
#     display_name: asf_mission_data_tool
#     language: python
#     name: python3
# ---

# %%
import pandas as pd
from asf_mission_data_tool.getters.data_getters import get_from_s3

# %%
# Heat Pump Deployment
# silver_key = "silver/heat_pump_deployment_quarterly_statistics/LATEST/Heat_pump_deployment_quarterly_statistics_United_Kingdom_2024_Q3_Table_1_2.parquet"
silver_key = "silver/heat_pump_deployment_quarterly_statistics/March_2025/Heat_pump_deployment_quarterly_statistics_United_Kingdom_2024_Q4_Table_1_3.parquet"

# %%
# UK territorial ghg emissions
silver_key = "silver/uk_territorial_greenhouse_gas_emissions_statistics/LATEST/final-greenhouse-gas-emissions-tables-2022_1_2.parquet"
# silver_key = "silver/uk_territorial_greenhouse_gas_emissions_statistics/LATEST/final-greenhouse-gas-emissions-tables-2022_7_1.parquet"

# %%
# Energy price cap
silver_key = "silver/energy_price_cap_levels/February_2025/Annex-9-Levelisation-allowance-methodology-and-levelised-cap-levels-v1.5_1c_Consumption_adjusted_levels.parquet"

# %%
# CB7
# silver_key = "silver/seventh_carbon_budget/LATEST/The-Seventh-Carbon-Budget-full-dataset_Economy_wide_data.parquet"
# silver_key = "silver/seventh_carbon_budget/LATEST/The-Seventh-Carbon-Budget-full-dataset_3_5.parquet"
# silver_key = "silver/seventh_carbon_budget/LATEST/The-Seventh-Carbon-Budget-full-dataset_7_2_1.parquet"
silver_key = "silver/seventh_carbon_budget/LATEST/The-Seventh-Carbon-Budget-full-dataset_7_2_4.parquet"

# %%
# EPCs
silver_key = "silver/energy_performance_of_buildings_certificates/LATEST/A1-_All_Properties_D1_England_Only.parquet"

# %%
# Check metadata was properly encoded
restored_dataframe = pd.read_parquet(get_from_s3(silver_key))
restored_dataframe

# %%
restored_dataframe.dtypes

# %%
restored_dataframe.attrs

# %%
restored_dataframe.isnull().sum()

# %%
