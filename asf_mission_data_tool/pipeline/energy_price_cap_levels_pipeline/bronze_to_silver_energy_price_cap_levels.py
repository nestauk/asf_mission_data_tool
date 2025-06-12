import pandas as pd

from asf_mission_data_tool.getters.data_getters import (
    get_from_s3,
    save_to_s3_silver,
    append_field_to_latest_version,
    get_latest_version,
)

from asf_mission_data_tool.getters.bronze_to_silver_transformations import (
    set_logger,
    convert_tariff_tables_to_tidy,
    convert_to_nan,
    assert_dtypes,
    expand_price_cap_period,
    rename_columns,
)


# Dataset specific variables
dataset_name = "energy_price_cap_levels"
latest_version = get_latest_version(dataset_name)
log_file_path = dataset_name + ".log"

# Set up logger for transformation tracking
set_logger(log_file_path)


# Check table-level information is available
tables = latest_version.get("tables")
if not tables:
    raise ValueError("No 'tables' key found in config, or 'tables' is empty.")

# Download file content from s3
latest_version_bronze_key = (
    get_latest_version(dataset_name)
    .get("file_bronze")[0]
    .replace("s3://asf-mission-data-tool/", "")
)
content = get_from_s3(s3_key=latest_version_bronze_key)


# Read and series transformations for each table in dataset
transformed_tables = {}
for table_dict in latest_version.get("tables"):

    # Extract information
    sheet_name = table_dict.get("sheet_name")
    if not sheet_name:
        raise ValueError("Missing 'sheet_name' in table data.")

    # Raise error if more tables are added
    if sheet_name not in ["1c Consumption adjusted levels"]:
        raise ValueError(
            "New tables have been added that are not currently accounted for in pipeline."
        )

    column_map = {
        "Payment method": "payment_method",
        "Fuel": "fuel",
        "Consumption": "consumption",
        "Tariff component": "tariff_component",
        "Price cap period": "price_cap_period",
        "Price cap period start": "price_cap_period_start",
        "Price cap period end": "price_cap_period_end",
        "value": "value",
    }

    table_df = (
        (
            pd.read_excel(content, sheet_name=sheet_name)
            .pipe(convert_tariff_tables_to_tidy, table_name=sheet_name)
            .pipe(
                convert_to_nan,
                column_name="value",
                to_replace="-",
                table_name=sheet_name,
            )
        )
        .pipe(
            expand_price_cap_period,
            column_name="Price cap period",
            table_name=sheet_name,
        )
        .pipe(rename_columns, table_name=sheet_name, column_map=column_map)
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

    file_path = save_to_s3_silver(
        dataframe=dataframe,
        dataset_name=dataset_name,
        main_file_dict=latest_version,
        file_url_index=0,
        subset_id=name.replace(" ", "_"),
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
