# %%
import pandas as pd
from datetime import datetime

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
    add_constant_column,
    melt_dataframe,
    rename_columns,
    remove_line_break_chars,
    expand_wave_season_dates,
    replace_nan_in_column,
    replace_value_in_column,
    assert_dtypes,
)

# %% [markdown]
# ## Time series tables

# %%
dataset_name = "public_attitudes_tracking_survey"
log_file_path = dataset_name + ".log"
set_logger(log_file_path)

# %% [markdown]
# ### Winter

# %%
winter_latest_version = get_latest_version(dataset_name, filter="Winter")

# %%
# Download file content from s3
winter_content = {}
for file_url in [
    url
    for url in winter_latest_version.get("file_url", [])
    if "time_series" in url.lower()
]:
    file_name = file_url.split("/")[-1]
    bronze_key = [
        file_bronze
        for file_bronze in winter_latest_version.get("file_bronze")
        if file_name in file_bronze
    ][0].replace("s3://asf-mission-data-tool/", "")
    winter_content[file_url] = get_from_s3(s3_key=bronze_key)

# %%
# Read all sheets into a dictionary of dataframes
winter_sheets = pd.read_excel(winter_content[file_url], sheet_name=None)

# Create question variable and full question lookup
winter_question_lookup = {}
for sheet in winter_sheets:
    if sheet != "Table of contents":
        winter_question_lookup[sheet] = winter_sheets[sheet].iloc[0, 0]

# %%
winter_sheets_transformed = {}
for sheet_name in winter_question_lookup.keys():
    df = winter_sheets[sheet_name]

    # Drop redundant rows and set header row
    df = df.iloc[7:].reset_index(drop=True)
    df.columns = df.iloc[0]
    df = df.drop(index=0)
    df = df.reset_index(drop=True)

    # Drop column for Total across all historical waves
    df = df.drop("Total", axis=1)

    # Add column for question label
    df = (
        add_constant_column(
            dataframe=df,
            table_name=f"winter/{sheet_name}",
            column_name="question",
            constant_value=winter_question_lookup[sheet_name],
        )
        .pipe(
            melt_dataframe,
            table_name=f"winter/{sheet_name}",
            id_vars=[
                "question",
                "Subgroup identifier row",
            ],
            var_name="wave_season",
            value_name="value",
        )
        .pipe(
            rename_columns,
            table_name=f"winter/{sheet_name}",
            column_map={"Subgroup identifier row": "variable"},
        )
    )

    # Add variable type column
    df["variable_type"] = df["variable"].apply(
        lambda x: (
            "count" if x == "Unweighted Base" or x == "Weighted Base" else "proportion"
        )
    )

    df = (
        remove_line_break_chars(
            dataframe=df,
            table_name=f"winter/{sheet_name}",
            column_to_clean="wave_season",
        )
        .pipe(
            expand_wave_season_dates,
            table_name=f"winter/{sheet_name}",
            column_name="wave_season",
        )
        .pipe(
            replace_nan_in_column,
            table_name=f"winter/{sheet_name}",
            column_name="value",
            value=0,
        )  # Where NA is used in the tables this denotes that the response was not given by any respondents.
        .pipe(
            replace_value_in_column,
            table_name=f"winter/{sheet_name}",
            column_name="value",
            replaced_value="low",
            replacement_value=0.5 / 100,
        )  # Where low is used in the tables this denotes that the response was given by less than 0.5% of the sample.
        .pipe(
            assert_dtypes,
            table_name=f"winter/{sheet_name}",
            expected_dtypes=winter_latest_version.get("tables")[0].get("dtypes"),
        )
    )

    winter_sheets_transformed[sheet_name] = df

# Combine all question tables
winter_master_table = pd.concat(
    winter_sheets_transformed.values(), ignore_index=True
).reset_index(drop=True)

# Reorder columns
winter_master_table = winter_master_table[
    [
        "question",
        "wave_year",
        "season_start_date",
        "season_end_date",
        "wave_season",
        "variable",
        "variable_type",
        "value",
    ]
]

# %% [markdown]
# ### Spring

# %%
spring_latest_version = get_latest_version(dataset_name, filter="Spring")

# %%
# Download file content from s3
spring_content = {}
for file_url in [
    url
    for url in spring_latest_version.get("file_url", [])
    if "time_series" in url.lower()
]:
    file_name = file_url.split("/")[-1]
    bronze_key = [
        file_bronze
        for file_bronze in spring_latest_version.get("file_bronze")
        if file_name in file_bronze
    ][0].replace("s3://asf-mission-data-tool/", "")
    spring_content[file_url] = get_from_s3(s3_key=bronze_key)

# %%
# Read all sheets into a dictionary of dataframes
spring_sheets = pd.read_excel(spring_content[file_url], sheet_name=None)

# Create question variable and full question lookup
spring_question_lookup = {}
for sheet in spring_sheets:
    if sheet != "Table of contents":
        spring_question_lookup[sheet] = spring_sheets[sheet].iloc[0, 0]

# %%
spring_sheets_transformed = {}
for sheet_name in spring_question_lookup.keys():
    df = spring_sheets[sheet_name]

    # Drop redundant rows and set header row
    df = df.iloc[7:].reset_index(drop=True)
    df.columns = df.iloc[0]
    df = df.drop(index=0)
    df = df.reset_index(drop=True)

    # Drop column for Total across all historical waves
    df = df.drop("Total", axis=1)

    # Add column for question label
    df = (
        add_constant_column(
            dataframe=df,
            table_name=f"spring/{sheet_name}",
            column_name="question",
            constant_value=spring_question_lookup[sheet_name],
        )
        .pipe(
            melt_dataframe,
            table_name=f"spring/{sheet_name}",
            id_vars=[
                "question",
                "Subgroup identifier row",
            ],
            var_name="wave_season",
            value_name="value",
        )
        .pipe(
            rename_columns,
            table_name=f"spring/{sheet_name}",
            column_map={"Subgroup identifier row": "variable"},
        )
    )

    # Add variable type column
    df["variable_type"] = df["variable"].apply(
        lambda x: (
            "count" if x == "Unweighted Base" or x == "Weighted Base" else "proportion"
        )
    )

    df = (
        remove_line_break_chars(
            dataframe=df,
            table_name=f"spring/{sheet_name}",
            column_to_clean="wave_season",
        )
        .pipe(
            expand_wave_season_dates,
            table_name=f"spring/{sheet_name}",
            column_name="wave_season",
        )
        .pipe(
            replace_nan_in_column,
            table_name=f"spring/{sheet_name}",
            column_name="value",
            value=0,
        )  # Where NA is used in the tables this denotes that the response was not given by any respondents.
        .pipe(
            replace_value_in_column,
            table_name=f"spring/{sheet_name}",
            column_name="value",
            replaced_value="low",
            replacement_value=0.5 / 100,
        )  # Where low is used in the tables this denotes that the response was given by less than 0.5% of the sample.
        .pipe(
            assert_dtypes,
            table_name=f"spring/{sheet_name}",
            expected_dtypes=spring_latest_version.get("tables")[0].get("dtypes"),
        )
    )

    spring_sheets_transformed[sheet_name] = df

# Combine all question tables
spring_master_table = pd.concat(
    spring_sheets_transformed.values(), ignore_index=True
).reset_index(drop=True)

# Reorder columns
spring_master_table = spring_master_table[
    [
        "question",
        "wave_year",
        "season_start_date",
        "season_end_date",
        "wave_season",
        "variable",
        "variable_type",
        "value",
    ]
]

# %% [markdown]
# ### Summer

# %%
summer_latest_version = get_latest_version(dataset_name, filter="Summer")

# %%
# Download file content from s3
summer_content = {}
for file_url in [
    url
    for url in summer_latest_version.get("file_url", [])
    if "time_series" in url.lower()
]:
    file_name = file_url.split("/")[-1]
    bronze_key = [
        file_bronze
        for file_bronze in summer_latest_version.get("file_bronze")
        if file_name in file_bronze
    ][0].replace("s3://asf-mission-data-tool/", "")
    summer_content[file_url] = get_from_s3(s3_key=bronze_key)

# %%
# Read all sheets into a dictionary of dataframes
summer_sheets = pd.read_excel(summer_content[file_url], sheet_name=None)

# Create question variable and full question lookup
summer_question_lookup = {}
for sheet in summer_sheets:
    if sheet != "Table of contents":
        summer_question_lookup[sheet] = summer_sheets[sheet].iloc[0, 0]

# %%
summer_sheets_transformed = {}
for sheet_name in summer_question_lookup.keys():
    df = summer_sheets[sheet_name]

    # Drop redundant rows and set header row
    df = df.iloc[7:].reset_index(drop=True)
    df.columns = df.iloc[0]
    df = df.drop(index=0)
    df = df.reset_index(drop=True)

    # Drop column for Total across all historical waves
    df = df.drop("Total", axis=1)

    # Add column for question label
    df = (
        add_constant_column(
            dataframe=df,
            table_name=f"summer/{sheet_name}",
            column_name="question",
            constant_value=summer_question_lookup[sheet_name],
        )
        .pipe(
            melt_dataframe,
            table_name=f"summer/{sheet_name}",
            id_vars=[
                "question",
                "Subgroup identifier row",
            ],
            var_name="wave_season",
            value_name="value",
        )
        .pipe(
            rename_columns,
            table_name=f"summer/{sheet_name}",
            column_map={"Subgroup identifier row": "variable"},
        )
    )

    # Add variable type column
    df["variable_type"] = df["variable"].apply(
        lambda x: (
            "count" if x == "Unweighted Base" or x == "Weighted Base" else "proportion"
        )
    )

    df = (
        remove_line_break_chars(
            dataframe=df,
            table_name=f"summer/{sheet_name}",
            column_to_clean="wave_season",
        )
        .pipe(
            expand_wave_season_dates,
            table_name=f"summer/{sheet_name}",
            column_name="wave_season",
        )
        .pipe(
            replace_nan_in_column,
            table_name=f"summer/{sheet_name}",
            column_name="value",
            value=0,
        )  # Where NA is used in the tables this denotes that the response was not given by any respondents.
        .pipe(
            replace_value_in_column,
            table_name=f"summer/{sheet_name}",
            column_name="value",
            replaced_value="low",
            replacement_value=0.5 / 100,
        )  # Where low is used in the tables this denotes that the response was given by less than 0.5% of the sample.
        .pipe(
            assert_dtypes,
            table_name=f"summer/{sheet_name}",
            expected_dtypes=summer_latest_version.get("tables")[0].get("dtypes"),
        )
    )

    summer_sheets_transformed[sheet_name] = df

# Combine all question tables
summer_master_table = pd.concat(
    summer_sheets_transformed.values(), ignore_index=True
).reset_index(drop=True)

# Reorder columns
summer_master_table = summer_master_table[
    [
        "question",
        "wave_year",
        "season_start_date",
        "season_end_date",
        "wave_season",
        "variable",
        "variable_type",
        "value",
    ]
]

# %% [markdown]
# ### Uploading to s3

# %%
# Add transformation log as metadata

winter_metadata = {}
with open(log_file_path, "r") as log_file:
    i = 1
    for line in log_file:
        if "winter" in line:
            winter_metadata[f"bronze_to_silver_transformation_{i}_winter"] = line
            i = i + 1
winter_master_table.attrs = winter_metadata

spring_metadata = {}
with open(log_file_path, "r") as log_file:
    i = 1
    for line in log_file:
        if "spring" in line:
            spring_metadata[f"bronze_to_silver_transformation_{i}_spring"] = line
            i = i + 1
spring_master_table.attrs = spring_metadata

summer_metadata = {}
with open(log_file_path, "r") as log_file:
    i = 1
    for line in log_file:
        if "summer" in line:
            summer_metadata[f"bronze_to_silver_transformation_{i}_summer"] = line
            i = i + 1
summer_master_table.attrs = summer_metadata

# %%
# Upload to s3

winter_silver_s3_file_path = save_to_s3_silver(
    dataframe=winter_master_table,
    dataset_name=dataset_name,
    main_file_dict=winter_latest_version,
    file_url_index=0,
    subset_id="all_questions",
)

spring_silver_s3_file_path = save_to_s3_silver(
    dataframe=spring_master_table,
    dataset_name=dataset_name,
    main_file_dict=spring_latest_version,
    file_url_index=0,
    subset_id="all_questions",
)

summer_silver_s3_file_path = save_to_s3_silver(
    dataframe=summer_master_table,
    dataset_name=dataset_name,
    main_file_dict=summer_latest_version,
    file_url_index=0,
    subset_id="all_questions",
)

# %%
# Append "file_silver" to config

# Winter
append_field_to_latest_version(
    dataset_name=dataset_name,
    s3_file_path=winter_silver_s3_file_path,
    new_field_name="file_silver",
    filter="Winter",
)

# Spring
append_field_to_latest_version(
    dataset_name=dataset_name,
    s3_file_path=spring_silver_s3_file_path,
    new_field_name="file_silver",
    filter="Spring",
)

# Summer
append_field_to_latest_version(
    dataset_name=dataset_name,
    s3_file_path=summer_silver_s3_file_path,
    new_field_name="file_silver",
    filter="Summer",
)
