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
    melt_dataframe,
    drop_nan_rows,
    drop_nan_columns,
    drop_duplicate_rows,
    rename_columns,
    drop_row,
    assert_dtypes,
    remove_special_chars,
    trim_whitespace,
    replace_value_in_column,
)

# %%
# Dataset specific variables
dataset_name = "english_housing_survey"
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
    if sheet_name not in ["AT2_5", "AT2_6", "AT2_7", "AT2_8"]:
        raise ValueError(
            "New tables have been added that are not currently accounted for in pipeline."
        )

    # Main heating system over time
    if sheet_name in ["AT2_5"]:
        table_df = (
            pd.read_excel(content[file_url], sheet_name=sheet_name, skiprows=skiprows)
            .pipe(drop_nan_rows, table_name=sheet_name, threshold=2)
            .pipe(drop_nan_columns, table_name=sheet_name)
            .pipe(
                rename_columns,
                table_name=sheet_name,
                column_map={"Unnamed: 1": "main_heating_system"},
            )
            .pipe(trim_whitespace, table_name=sheet_name)
            .reset_index(drop=True)
        )

        # Add units column
        unit_map = {
            "central heating": "thousands of dwellings",
            "storage heater": "thousands of dwellings",
            "fixed room/portable heater": "thousands of dwellings",
            "total": "thousands of dwellings",
            "sample size": "dwellings",
        }
        percent_rows = [
            "central heating",
            "storage heater",
            "fixed room/portable heater",
            "total",
        ]
        percentage_start_index = (
            table_df[table_df["main_heating_system"] == "total"].index[0] + 1
        )
        for idx in table_df.index:
            if (
                idx >= percentage_start_index
                and table_df.loc[idx, "main_heating_system"] in percent_rows
            ):
                unit = "percentages"
            else:
                unit = unit_map.get(table_df.loc[idx, "main_heating_system"])
            table_df.loc[idx, "unit"] = unit

        table_df = melt_dataframe(
            dataframe=table_df,
            table_name=sheet_name,
            id_vars=["main_heating_system", "unit"],
            value_name="value",
            var_name="year",
        ).pipe(
            assert_dtypes,
            table_name=sheet_name,
            expected_dtypes=table_dict.get("dtypes"),
        )

    # Main heating system by tenure in latest year
    if sheet_name in ["AT2_6"]:
        table_df = (
            pd.read_excel(content[file_url], sheet_name=sheet_name, skiprows=skiprows)
            .pipe(drop_nan_rows, table_name=sheet_name, threshold=2)
            .pipe(drop_nan_columns, table_name=sheet_name)
            .pipe(
                rename_columns,
                table_name=sheet_name,
                column_map={"Unnamed: 1": "tenure"},
            )
            .pipe(trim_whitespace, table_name=sheet_name)
            .reset_index(drop=True)
        )
        # Rearrange sample size column to ensure different unit
        table_df_cut = table_df.dropna(axis="rows", thresh=5)
        table_df = table_df.drop("sample size", axis=1)
        table_df = pd.concat(
            [table_df, table_df_cut[["tenure", "sample size"]]]
        ).reset_index(drop=True)

        # Add units column
        tenure_labels = [
            "owner occupied",
            "private rented",
            "all private sector",
            "local authority",
            "housing association",
            "all social sector",
            "all tenures",
        ]
        percentage_start_index = (
            table_df[table_df["tenure"] == "all tenures"].index[0] + 1
        )
        sample_size_start_index = (
            table_df[table_df["tenure"] == "all tenures"].index[1] + 1
        )
        for idx in table_df.index:
            if idx >= percentage_start_index and idx < sample_size_start_index:
                unit = "percentages"
            elif idx >= sample_size_start_index:
                unit = "dwellings"
            else:
                unit = "thousands of dwellings"
            table_df.loc[idx, "unit"] = unit

        table_df = rename_columns(
            dataframe=table_df,
            table_name=sheet_name,
            column_map={
                "centralheating": "central heating",
                "storageheater": "storage heater",
            },
        ).pipe(
            melt_dataframe,
            table_name=sheet_name,
            id_vars=["tenure", "unit"],
            value_name="value",
            var_name="variable",
        )

        # Drop redundant rows
        table_df = drop_duplicate_rows(
            dataframe=table_df,
            table_name=sheet_name,
            consider_columns=["tenure", "variable", "value"],
            keep=False,
        ).pipe(
            drop_duplicate_rows,
            table_name=sheet_name,
            consider_columns=["tenure", "unit", "value"],
            keep=False,
        )

        table_df = assert_dtypes(
            dataframe=table_df,
            table_name=sheet_name,
            expected_dtypes=table_dict.get("dtypes"),
        )

    # Boiler types over time
    if sheet_name in ["AT2_7"]:
        table_df = (
            pd.read_excel(content[file_url], sheet_name=sheet_name, skiprows=skiprows)
            .pipe(drop_nan_rows, table_name=sheet_name, threshold=2)
            .pipe(drop_nan_columns, table_name=sheet_name)
            .pipe(
                rename_columns,
                table_name=sheet_name,
                column_map={"Unnamed: 1": "boiler_type"},
            )
            .pipe(trim_whitespace, table_name=sheet_name)
            .reset_index(drop=True)
        )

        # Add units column
        unit_map = {
            "standard boiler": "thousands of dwellings",
            "back boiler": "thousands of dwellings",
            "combination boiler": "thousands of dwellings",
            "condensing boiler": "thousands of dwellings",
            "condensing-combination boiler": "thousands of dwellings",
            "no boiler": "thousands of dwellings",
            "total": "thousands of dwellings",
            "sample size": "dwellings",
        }
        percent_rows = [
            "standard boiler",
            "back boiler",
            "combination boiler",
            "condensing boiler",
            "condensing-combination boiler",
            "no boiler" "total",
        ]
        percentage_start_index = (
            table_df[table_df["boiler_type"] == "total"].index[0] + 1
        )
        for idx in table_df.index:
            if (
                idx >= percentage_start_index
                and table_df.loc[idx, "boiler_type"] in percent_rows
            ):
                unit = "percentages"
            else:
                unit = unit_map.get(table_df.loc[idx, "boiler_type"])
            table_df.loc[idx, "unit"] = unit

        table_df = (
            melt_dataframe(
                dataframe=table_df,
                table_name=sheet_name,
                id_vars=["boiler_type", "unit"],
                value_name="value",
                var_name="year",
            )
            .pipe(
                replace_value_in_column,
                table_name=sheet_name,
                column_name="value",
                replaced_value="-",
                replacement_value=None,
            )
            .pipe(
                assert_dtypes,
                table_name=sheet_name,
                expected_dtypes=table_dict.get("dtypes"),
            )
        )

    # Boiler type by tenure in latest year
    if sheet_name in ["AT2_8"]:
        table_df = (
            pd.read_excel(content[file_url], sheet_name=sheet_name, skiprows=skiprows)
            .pipe(drop_nan_rows, table_name=sheet_name, threshold=2)
            .pipe(drop_nan_columns, table_name=sheet_name)
            .pipe(
                rename_columns,
                table_name=sheet_name,
                column_map={"Unnamed: 1": "tenure"},
            )
            .pipe(trim_whitespace, table_name=sheet_name)
            .reset_index(drop=True)
        )

        # Rearrange sample size column to ensure different unit
        table_df_cut = table_df[table_df["samplesize"] != 100.0]
        table_df = table_df.drop("samplesize", axis=1)
        table_df = pd.concat(
            [table_df, table_df_cut[["tenure", "samplesize"]]]
        ).reset_index(drop=True)

        # Add units column
        tenure_labels = [
            "owner occupied",
            "private rented",
            "all private sector",
            "local authority",
            "housing association",
            "all social sector",
            "all tenures",
        ]
        percentage_start_index = (
            table_df[table_df["tenure"] == "all tenures"].index[0] + 1
        )
        sample_size_start_index = (
            table_df[table_df["tenure"] == "all tenures"].index[1] + 1
        )
        for idx in table_df.index:
            if idx >= percentage_start_index and idx < sample_size_start_index:
                unit = "percentages"
            elif idx >= sample_size_start_index:
                unit = "dwellings"
            else:
                unit = "thousands of dwellings"
            table_df.loc[idx, "unit"] = unit

        table_df = (
            rename_columns(
                dataframe=table_df,
                table_name=sheet_name,
                column_map={
                    "backboiler": "back boiler",
                    "noboiler": "no boiler",
                    "samplesize": "sample size",
                },
            )
            .pipe(
                melt_dataframe,
                table_name=sheet_name,
                id_vars=["tenure", "unit"],
                value_name="value",
                var_name="variable",
            )
            .pipe(
                replace_value_in_column,
                table_name=sheet_name,
                column_name="value",
                replaced_value="u",  # disclosive, findings derived from unweighted cell counts of <5 and >0
                replacement_value=None,
            )
        )

        # Drop redundant rows
        table_df = drop_duplicate_rows(
            dataframe=table_df,
            table_name=sheet_name,
            consider_columns=["tenure", "variable", "value"],
            keep=False,
        ).pipe(
            drop_duplicate_rows,
            table_name=sheet_name,
            consider_columns=["tenure", "unit", "value"],
            keep=False,
        )

        table_df = assert_dtypes(
            dataframe=table_df,
            table_name=sheet_name,
            expected_dtypes=table_dict.get("dtypes"),
        )

    transformed_tables[sheet_name] = table_df

# %%
# # Load to s3 with transformation log as metadata
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
        subset_id=name,
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
