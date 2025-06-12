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

# %%
from asf_mission_data_tool.getters.data_getters import (
    get_from_s3,
    save_to_s3_silver,
    append_field_to_latest_version,
    get_latest_version,
)

# %%
from asf_mission_data_tool.getters.bronze_to_silver_transformations import (
    set_logger,
    forward_fill_nan_by_column,
    fill_sector_category_columns,
    check_missing_values,
    assert_dtypes,
    melt_dataframe,
    convert_year_column_to_int,
    rename_columns,
    add_constant_column,
)

# %%
# Dataset specific variables
dataset_name = "uk_territorial_greenhouse_gas_emissions_statistics"
latest_version = get_latest_version(dataset_name, filter="final")
log_file_path = dataset_name + ".log"

# %%
# Set up logger for transformation tracking
set_logger(log_file_path)

# %%
# Check table-level information is available
tables = latest_version.get("tables")
if not tables:
    raise ValueError("No 'tables' key found in config, or 'tables' is empty.")

# %%
# Download file content from s3
content = {}
for file_url in latest_version.get("file_url"):
    file_name = file_url.split("/")[-1]
    bronze_key = [
        file_bronze
        for file_bronze in latest_version.get("file_bronze")
        if file_name in file_bronze
    ][0].replace("s3://asf-mission-data-tool/", "")
    content[file_url] = get_from_s3(s3_key=bronze_key)

# %%
# Read and series transformations for each table in dataset
transformed_tables = {}
for table_dict in latest_version.get("tables"):

    # Extract information
    sheet_name = table_dict.get("sheet_name")
    if not sheet_name:
        raise ValueError("Missing 'sheet_name' in table data.")

    skiprows = table_dict.get("skiprows")
    if not skiprows:
        raise ValueError("Missing 'skiprows' in table data.")

    dtypes = table_dict.get("dtypes")

    file_url = table_dict.get("file_url")

    # Raise error if more tables are added
    if sheet_name not in ["1.2", "7.1"]:
        raise ValueError(
            "New tables have been added that are not currently accounted for in pipeline."
        )

    table_df = (
        pd.read_excel(content[file_url], sheet_name=sheet_name, skiprows=skiprows)
        .pipe(
            forward_fill_nan_by_column,
            table_name=sheet_name,
            column_name="TES sector",
        )
        .pipe(
            forward_fill_nan_by_column,
            table_name=sheet_name,
            column_name="TES subsector",
        )
    ).pipe(
        fill_sector_category_columns,
        table_name=sheet_name,
        sector_column="TES sector",
        subsector_column="TES subsector",
        category_column="TES category",
    )

    column_map = {
        "TES sector": "tes_sector",
        "TES subsector": "tes_subsector",
        "TES category": "tes_category",
        "Year": "year",
        "Territorial greenhouse gas emissions (MtCO2e)": "territorial_greenhouse_gas_emissions",
    }

    table_df = (
        table_df.pipe(check_missing_values, table_name=sheet_name)
        .pipe(
            melt_dataframe,
            table_name=sheet_name,
            id_vars=[
                "TES sector",
                "TES subsector",
                "TES category",
            ],  # These were TES Sector, TES Subsector, TES Category in 2022 version
            var_name="Year",
            value_name="Territorial greenhouse gas emissions (MtCO2e)",
        )
        .pipe(
            convert_year_column_to_int, table_name=sheet_name, year_column_name="Year"
        )
        .pipe(rename_columns, table_name=sheet_name, column_map=column_map)
        .pipe(
            add_constant_column,
            table_name=sheet_name,
            column_name="unit",
            constant_value="MtCO2e",
        )
        .pipe(
            assert_dtypes,
            table_name=sheet_name,
            expected_dtypes=table_dict.get("dtypes"),
        )
    )
    transformed_tables[sheet_name] = table_df

# %%
# Load to s3 with transformation log as metadata
s3_file_path = []
for name, dataframe in transformed_tables.items():

    metadata = {}
    with open(log_file_path, "r") as log_file:
        i = 1
        for line in log_file:
            if name in line:
                metadata[f"bronze_to_silver_transformation_{i}"] = line
                i = i + 1

    # Encode metadata to parquet metadata via DataFrame attrs property
    dataframe.attrs = metadata

    # Get corresponding file url of table
    for dict in latest_version.get("tables"):
        if dict["sheet_name"] == name:
            file_url = dict["file_url"]

    file_path = save_to_s3_silver(
        dataframe=dataframe,
        dataset_name=dataset_name,
        main_file_dict=latest_version,
        file_url_index=latest_version["file_url"].index(file_url),
        subset_id=name.replace(".", "_"),
    )
    s3_file_path.append(file_path)

# %%
# Append "file_silver" to config
if not s3_file_path:
    pass
else:
    append_field_to_latest_version(
        dataset_name=dataset_name,
        s3_file_path=s3_file_path,
        new_field_name="file_silver",
        filter="final",
    )
