import pandas as pd

from asf_mission_data_tool.getters.data_getters import (
    get_from_s3,
    save_to_s3_silver,
    append_field_to_latest_version,
    get_latest_version,
)

from asf_mission_data_tool.getters.bronze_to_silver_transformations import (
    set_logger,
    assert_dtypes,
    drop_nan_columns,
    drop_row,
    add_constant_column,
    melt_dataframe,
    replace_nan_in_column,
    convert_sign_of_abatement,
    replace_value_in_column,
    rename_columns,
)

# Dataset specific variables
dataset_name = "seventh_carbon_budget"
latest_version = get_latest_version(dataset_name)
log_file_path = dataset_name + ".log"

# Set up logger for transformation tracking
set_logger(log_file_path)

# Check table-level information is available
tables = latest_version.get("tables")
if not tables:
    raise ValueError("No 'tables' key found in config, or 'tables' is empty.")

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

# Read and series transformations for each table in dataset
transformed_tables = {}
for table_dict in latest_version.get("tables"):

    # Extract information
    sheet_name = table_dict.get("sheet_name")
    if not sheet_name:
        raise ValueError("Missing 'sheet_name' in table data.")
    file_url = table_dict.get("file_url")

    # Raise error if more tables are added
    if sheet_name not in [
        "Economy-wide data",
        "Sector-level data",
        "Subsector-level data",
        "Measure-level data",
        "3.5",
        "7.2.1",
        "7.2.2",
        "7.2.4",
    ]:
        raise ValueError(
            "New tables have been added that are not currently accounted for in pipeline."
        )

    # Raw data tables
    if sheet_name in [
        "Economy-wide data",
        "Sector-level data",
        "Subsector-level data",
        "Measure-level data",
    ]:
        table_df = pd.read_excel(content[file_url], sheet_name=sheet_name).pipe(
            assert_dtypes,
            table_name=sheet_name,
            expected_dtypes=table_dict.get("dtypes"),
        )

    # Chart data tables
    if sheet_name in ["3.5"]:  # Sources of abatement in Balanced Pathway

        skiprows = table_dict.get("skiprows")
        if not skiprows:
            raise ValueError("Missing 'skiprows' in table data.")

        redundant_rows = [0, 1, 2, 3, 20, 21, 22]

        table_df = (
            pd.read_excel(content[file_url], sheet_name=sheet_name, skiprows=skiprows)
            .pipe(drop_nan_columns, table_name=sheet_name)
            .pipe(drop_row, table_name=sheet_name, index_list_to_drop=redundant_rows)
            .pipe(
                add_constant_column,
                table_name=sheet_name,
                column_name="Pathway",
                constant_value="Balanced Pathway",
            )
            .pipe(
                melt_dataframe,
                table_name=sheet_name,
                id_vars=["Pathway", "Category", "Series"],
                var_name="Year",
                value_name="Value",
            )
            .pipe(
                replace_nan_in_column,
                table_name=sheet_name,
                column_name="Category",
                value="Residual emissions",
            )
            .pipe(
                replace_nan_in_column,
                table_name=sheet_name,
                column_name="Value",
                value=0,
            )
            .pipe(convert_sign_of_abatement, table_name=sheet_name)
            .pipe(
                assert_dtypes,
                table_name=sheet_name,
                expected_dtypes=table_dict.get("dtypes"),
            )
        )

    if sheet_name in [
        "7.2.1"
    ]:  # Residential buildings emissions, historical and Balanced Pathway

        skiprows = table_dict.get("skiprows")
        if not skiprows:
            raise ValueError("Missing 'skiprows' in table data.")

        redundant_rows = [9]

        table_df = (
            pd.read_excel(content[file_url], sheet_name=sheet_name, skiprows=skiprows)
            .pipe(drop_nan_columns, table_name=sheet_name)
            .pipe(drop_row, table_name=sheet_name, index_list_to_drop=redundant_rows)
            .pipe(
                melt_dataframe,
                table_name=sheet_name,
                id_vars=["Series", "Subsector"],
                var_name="Year",
                value_name="Emissions (MtCO2e)",
            )
            .pipe(
                replace_value_in_column,
                table_name=sheet_name,
                column_name="Emissions (MtCO2e)",
                replaced_value=-10000,
                replacement_value=0,
            )
            .pipe(
                assert_dtypes,
                table_name=sheet_name,
                expected_dtypes=table_dict.get("dtypes"),
            )
        )

    if sheet_name in [
        "7.2.2"
    ]:  # Sources of abatement in Balanced Pathway for residential buildings

        skiprows = table_dict.get("skiprows")
        if not skiprows:
            raise ValueError("Missing 'skiprows' in table data.")

        redundant_rows = [1, 7, 8, 9, 10]

        column_map = {"Unnamed: 10": "Category"}

        table_df = (
            pd.read_excel(content[file_url], sheet_name=sheet_name, skiprows=skiprows)
            .pipe(drop_nan_columns, table_name=sheet_name)
            .pipe(drop_row, table_name=sheet_name, index_list_to_drop=redundant_rows)
            .pipe(rename_columns, table_name=sheet_name, column_map=column_map)
            .pipe(
                add_constant_column,
                table_name=sheet_name,
                column_name="Pathway",
                constant_value="Balanced Pathway",
            )
            .pipe(
                melt_dataframe,
                table_name=sheet_name,
                id_vars=["Pathway", "Category"],
                var_name="Year",
                value_name="Value",
            )
            .pipe(
                replace_nan_in_column,
                table_name=sheet_name,
                column_name="Category",
                value="Residual emissions",
            )
            .pipe(
                replace_nan_in_column,
                table_name=sheet_name,
                column_name="Value",
                value=0,
            )
            .pipe(convert_sign_of_abatement, table_name=sheet_name)
            .pipe(
                assert_dtypes,
                table_name=sheet_name,
                expected_dtypes=table_dict.get("dtypes"),
            )
        )
    if sheet_name in ["7.2.4"]:  # Key indicators for residential buildings

        skiprows = table_dict.get("skiprows")
        if not skiprows:
            raise ValueError("Missing 'skiprows' in table data.")

        table_df = (
            pd.read_excel(
                content[file_url],
                sheet_name=sheet_name,
                skiprows=skiprows,
            )
            .pipe(drop_nan_columns, table_name=sheet_name)
            .pipe(
                replace_nan_in_column,
                table_name=sheet_name,
                column_name="Unit",
                value="-",
            )
            .pipe(
                melt_dataframe,
                table_name=sheet_name,
                id_vars=["Name", "Unit", "Series"],
                var_name="Year",
                value_name="Value",
            )
            .pipe(
                assert_dtypes,
                table_name=sheet_name,
                expected_dtypes=table_dict.get("dtypes"),
            )
        )

    transformed_tables[sheet_name] = table_df

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
        subset_id=name.replace(" ", "_").replace("-", "_").replace(".", "_"),
    )
    s3_file_path.append(file_path)

# Append "file_silver" to config
if not s3_file_path:
    pass
else:
    append_field_to_latest_version(
        dataset_name=dataset_name,
        s3_file_path=s3_file_path,
        new_field_name="file_silver",
    )
