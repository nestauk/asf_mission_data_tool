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
    rename_columns,
    drop_row,
    extract_substring_in_column,
    melt_dataframe,
    assert_dtypes,
)

# %%
# Dataset specific variables
dataset_name = "energy_performance_of_buildings_certificates"
latest_version = get_latest_version(dataset_name)
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
    if sheet_name not in [
        "D1_England_Only",
        "D1_Wales_Only",
        "D2_England_Only",
        "D2_Wales_Only",
        "D3_England_Only",
        "D3_Wales_Only",
    ]:
        raise ValueError(
            "New tables have been added that are not currently accounted for in pipeline."
        )

    if "D1" in sheet_name:  # Energy Efficiency Rating
        column_map = {
            "A": "Energy Effiency Rating A",
            "B": "Energy Effiency Rating B",
            "C": "Energy Effiency Rating C",
            "D": "Energy Effiency Rating D",
            "E": "Energy Effiency Rating E",
            "F": "Energy Effiency Rating F",
            "G": "Energy Effiency Rating G",
            "Not Recorded": "Energy Effiency Rating Not Recorded",
        }

    elif "D2" in sheet_name:  # Environmental Impact Rating
        column_map = {
            "A": "Environmental Impact Rating A",
            "B": "Environmental Impact Rating B",
            "C": "Environmental Impact Rating C",
            "D": "Environmental Impact Rating D",
            "E": "Environmental Impact Rating E",
            "F": "Environmental Impact Rating F",
            "G": "Environmental Impact RatingR G",
            "Not Recorded": "Environmental Impact Rating Not Recorded",
        }
    else:
        column_map = {}

    table_df = pd.read_excel(
        content[file_url], sheet_name=sheet_name, skiprows=skiprows
    ).pipe(rename_columns, table_name=sheet_name, column_map=column_map)

    nan_quarter_indices = table_df.loc[pd.isna(table_df["Quarter"]), :].index.tolist()

    table_df = (
        table_df.pipe(
            drop_row,
            table_name=sheet_name,
            index_list_to_drop=nan_quarter_indices,
        )
        .pipe(
            extract_substring_in_column,
            table_name=sheet_name,
            separator="/",
            column_name="Quarter",
            segment_to_extract=1,
        )
        .pipe(
            melt_dataframe,
            table_name=sheet_name,
            id_vars=["Year", "Quarter"],
            var_name="Variable",
            value_name="Value",
        )
        .pipe(assert_dtypes, table_name=sheet_name, expected_dtypes=dtypes)
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
        subset_id=name.replace(" ", "_"),
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
    )
