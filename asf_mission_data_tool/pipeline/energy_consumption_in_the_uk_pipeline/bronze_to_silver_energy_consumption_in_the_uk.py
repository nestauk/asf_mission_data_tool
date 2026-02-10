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
    add_year_column,
    rename_columns,
    drop_row,
    melt_dataframe,
    assert_dtypes,
    replace_value_in_column,
    remove_line_break_chars,
)

# %%
# Dataset specific variables
dataset_name = "energy_consumption_in_the_uk"
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
    if sheet_name not in ["Table C1", "Table C9"]:
        raise ValueError(
            "New tables have been added that are not currently accounted for in pipeline."
        )

    table_df = pd.read_excel(
        content[file_url], sheet_name=sheet_name, skiprows=skiprows, header=[0, 1]
    )

    if sheet_name == "Table C1":
        table_df = (
            (
                add_year_column(
                    dataframe=table_df,
                    table_name=sheet_name,
                    year_column_name=("Year", "Year"),
                    existing_year_column_to_duplicate=("Industry [Note 1]", "Year"),
                )
                .pipe(
                    melt_dataframe,
                    table_name=sheet_name,
                    id_vars=[("Year", "Year")],
                    value_vars=table_df.columns.tolist(),
                    value_name="energy_consumption_ktoe",
                )
                .pipe(
                    rename_columns,
                    table_name=sheet_name,
                    column_map={
                        ("Year", "Year"): "year",
                        "variable_0": "sector",
                        "variable_1": "category",
                    },
                )
            )
            .pipe(
                remove_line_break_chars, table_name=sheet_name, column_to_clean="sector"
            )
            .pipe(
                remove_line_break_chars,
                table_name=sheet_name,
                column_to_clean="category",
            )
        )

        redundant_year_rows_in_sector_col = table_df.loc[
            table_df["sector"].str.contains("Year", na=False)
        ].index.tolist()

        table_df = drop_row(
            dataframe=table_df,
            table_name=sheet_name,
            index_list_to_drop=redundant_year_rows_in_sector_col,
        )

        redundant_year_rows_in_category_col = table_df.loc[
            table_df["category"].str.contains("Year", na=False)
        ].index.tolist()
        table_df = drop_row(
            dataframe=table_df,
            table_name=sheet_name,
            index_list_to_drop=redundant_year_rows_in_category_col,
        )

        table_df = replace_value_in_column(
            dataframe=table_df,
            table_name=sheet_name,
            column_name="energy_consumption_ktoe",
            replaced_value="[x]",
            replacement_value=None,
        )

    if sheet_name == "Table C9":
        table_df = (
            add_year_column(
                dataframe=table_df,
                table_name=sheet_name,
                year_column_name=("Year", "Year"),
                existing_year_column_to_duplicate=("Electricity", "Year"),
            )
            .pipe(
                melt_dataframe,
                table_name=sheet_name,
                id_vars=[("Year", "Year")],
                value_vars=table_df.columns.tolist(),
                value_name="value",
            )
            .pipe(
                rename_columns,
                table_name=sheet_name,
                column_map={
                    ("Year", "Year"): "year",
                    "variable_0": "fuel",
                    "variable_1": "variable",
                },
            )
        ).pipe(
            remove_line_break_chars, table_name=sheet_name, column_to_clean="variable"
        )

        redundant_year_rows_in_fuel_col = table_df.loc[
            table_df["fuel"].str.contains("Year", na=False)
        ].index.tolist()
        table_df = drop_row(
            dataframe=table_df,
            table_name=sheet_name,
            index_list_to_drop=redundant_year_rows_in_fuel_col,
        )

        redundant_year_rows_in_variable_col = table_df.loc[
            table_df["variable"].str.contains("Year", na=False)
        ].index.tolist()
        table_df = drop_row(
            dataframe=table_df,
            table_name=sheet_name,
            index_list_to_drop=redundant_year_rows_in_variable_col,
        )

    table_df = assert_dtypes(
        dataframe=table_df, table_name=sheet_name, expected_dtypes=dtypes
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

    file_path = save_to_s3_silver(
        dataframe=dataframe,
        dataset_name=dataset_name,
        main_file_dict=latest_version,
        file_url_index=0,
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
