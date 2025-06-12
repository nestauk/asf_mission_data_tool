import pandas as pd

from asf_mission_data_tool.getters.data_getters import (
    get_from_s3,
    save_to_s3_silver,
    append_field_to_latest_version,
    get_latest_version,
)

from asf_mission_data_tool.getters.bronze_to_silver_transformations import (
    set_logger,
    standardise_column_names,
    check_missing_values,
    assert_dtypes,
    expand_quarter_string,
    melt_dataframe,
    rename_columns,
)

# Dataset specific variables
dataset_name = "heat_pump_deployment_quarterly_statistics"
latest_version = get_latest_version(dataset_name)
log_file_path = dataset_name + ".log"

# Set up logger for transformation tracking
set_logger(log_file_path)

# Download file content from s3
latest_version_bronze_key = (
    get_latest_version(dataset_name)
    .get("file_bronze")[0]
    .replace("s3://asf-mission-data-tool/", "")
)
content = get_from_s3(s3_key=latest_version_bronze_key)

# Check table-level information is available
tables = latest_version.get("tables")
if not tables:
    raise ValueError("No 'tables' key found in config, or 'tables' is empty.")

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

    # Raise error if more tables are added
    if sheet_name not in ["Table 1.1", "Table 1.2", "Table 1.3"]:
        raise ValueError(
            "New tables have been added that are not currently accounted for in pipeline."
        )

    type_lookup = {
        "Table 1.1": "government_scheme",
        "Table 1.2": "technology_type",
        "Table 1.3": "region",
    }
    column_map = {
        "Installation quarter [note 4]": "installation_quarter",
        "Year": "year",
        "Start of quarter": "start_of_quarter",
        "End of quarter": "end_of_quarter",
        type_lookup.get(sheet_name): type_lookup.get(sheet_name),
        "Installations": "installations",
    }

    table_df = (
        pd.read_excel(content, sheet_name=sheet_name, skiprows=skiprows)
        .pipe(standardise_column_names, table_name=sheet_name)
        .pipe(check_missing_values, table_name=sheet_name)
        .pipe(expand_quarter_string, table_name=sheet_name)
        .pipe(
            melt_dataframe,
            table_name=sheet_name,
            id_vars=[
                "Installation quarter [note 4]",
                "Year",
                "Start of quarter",
                "End of quarter",
            ],
            var_name=type_lookup.get(sheet_name),
            value_name="Installations",
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
        subset_id="Table_" + name.split(" ")[1].replace(".", "_"),
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
