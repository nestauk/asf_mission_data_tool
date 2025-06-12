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
)

# %%
from asf_mission_data_tool.getters.bronze_to_silver_transformations import (
    drop_nan_rows,
    drop_row,
    melt_dataframe,
    rename_columns,
    assert_dtypes,
    add_constant_column,
    remove_line_break_chars,
    remove_special_chars,
    forward_fill_nan_by_column,
    replace_value_in_column,
    forward_backward_fill_nan_by_column,
    drop_duplicate_rows,
)

# %%
# Dataset specific variables
dataset_name = "fuel_poverty_statistics"
latest_version = get_latest_version(dataset_name)
log_file_path = dataset_name + ".log"

# %%
# Set up logger for transformation tracking
set_logger(log_file_path)

# %%
# Download file content from s3
latest_version_bronze_key = (
    get_latest_version(dataset_name)
    .get("file_bronze")[0]
    .replace("s3://asf-mission-data-tool/", "")
)
content = get_from_s3(s3_key=latest_version_bronze_key)

# %%
# Check table-level information is available
tables = latest_version.get("tables")
if not tables:
    raise ValueError("No 'tables' key found in config, or 'tables' is empty.")

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

    # Raise error if more tables are added
    if sheet_name not in [
        "Table 1",  # By all households and vulnerable households
        "Table 2",  # By quadrant of Low Income Low Energy Efficiency matrix
        "Table 3",  # By Fuel Poverty Energy Efficiency Rating FPEER
        "Table 5",  # By rurality and FPEER
        "Table 6",  # By region
        "Table 7",  # By dwelling type
        "Table 8",  # By age of dwelling
        "Table 9",  # By floor area
        "Table 10",  # By gas grid connection and FPEER
        "Table 11",  # By central heating and FPEER
        "Table 12",  # By main fuel type and FPEER
        "Table 13",  # By central heating and main fuel
        "Table 14",  # By boiler type
        "Table 15",  # By wall insulation and FPEER
        "Table 16",  # By wall type and gas grid connection
        "Table 17",  # By loft insulation
        "Table 18",  # By renewable heat technology and FPEER
        "Table 19",  # By tenure and FPEER
        "Table 20",  # By housing sector
        "Table 24",  # By number of people in household
        "Table 29",  # By tenure and vulnerability
        "Table 31",  # By income decile and FPEER
        "Table 32",  # By gas payment method
        "Table 33",  # By electricity payment method
        "Table 34",  # By in receipt of benefits
        "Table 35",  # By ECO4 Help to Heat Group eligibility
    ]:
        raise ValueError(
            "New tables have been added that are not currently accounted for in pipeline."
        )

    table_df = pd.read_excel(content, sheet_name=sheet_name, skiprows=skiprows)

    # Layout type: Two separate tables with same fields but different household groups
    if sheet_name == "Table 1":

        table_df = drop_nan_rows(
            dataframe=table_df, table_name=sheet_name, threshold=3
        ).reset_index(drop=True)

        table_df["category"] = [
            "All households",
            "All households",
            "All households",
            "Vulnerable households only",
            "Vulnerable households only",
            "Vulnerable households only",
            "Vulnerable households only",
        ]

        row_to_drop = (
            table_df["All households"].to_list().index("Vulnerable households only")
        )
        table_df = drop_row(
            dataframe=table_df, table_name=sheet_name, index_list_to_drop=row_to_drop
        )

        table_df = (
            melt_dataframe(
                dataframe=table_df,
                table_name=sheet_name,
                id_vars=["category", "All households"],
                value_name="value",
            )
            .pipe(drop_nan_rows, table_name=sheet_name, threshold=4)
            .reset_index(drop=True)
            .pipe(
                rename_columns,
                table_name=sheet_name,
                column_map={"All households": "group"},
            )
            .pipe(
                assert_dtypes,
                table_name=sheet_name,
                expected_dtypes=table_dict.get("dtypes"),
            )
        )

    # Layout type: Single table
    if sheet_name in [
        "Table 2",
        "Table 3",
        "Table 6",
        "Table 7",
        "Table 8",
        "Table 9",
        "Table 14",
        "Table 17",
        "Table 20",
        "Table 24",
        "Table 32",
        "Table 33",
        "Table 35",
    ]:
        category_name = table_df.columns.to_list()[0]
        table_df = (
            drop_nan_rows(dataframe=table_df, table_name=sheet_name, threshold=2)
            .reset_index(drop=True)
            .pipe(
                add_constant_column,
                table_name=sheet_name,
                column_name="category",
                constant_value=category_name,
            )
            .pipe(
                melt_dataframe,
                table_name=sheet_name,
                id_vars=["category", category_name],
                value_name="value",
            )
            .pipe(
                rename_columns,
                table_name=sheet_name,
                column_map={category_name: "group"},
            )
            .pipe(
                remove_line_break_chars,
                table_name=sheet_name,
                column_to_clean="category",
            )
            .pipe(
                remove_special_chars,
                table_name=sheet_name,
                column_to_clean="value",
                char_to_remove=r"\^",  # ^ Numbers based on a low sample count (between 10 and less than 30) have been marked with ^. Inferences should not be made on this figure.
            )
            .pipe(
                replace_value_in_column,
                table_name=sheet_name,
                column_name="value",
                replaced_value="*",  # * Numbers based on very low sample count (less than 10) have been hidden with *.
                replacement_value=0.0,
            )
            .pipe(
                assert_dtypes,
                table_name=sheet_name,
                expected_dtypes=table_dict.get("dtypes"),
            )
        )

    # Layout type: Two tables with total and type breakdown in second table
    if sheet_name in [
        "Table 5",
        "Table 10",
        "Table 11",
        "Table 12",
        "Table 18",
        "Table 31",
    ]:
        category_1_name = table_df.columns.to_list()[0]
        table_df = (
            drop_nan_rows(dataframe=table_df, table_name=sheet_name, threshold=2)
            .reset_index(drop=True)
            .pipe(
                forward_fill_nan_by_column,
                table_name=sheet_name,
                column_name=category_1_name,
            )
        )

        # Read full table only
        drop_row_lookup = {
            "Table 5": 4,
            "Table 10": 3,
            "Table 11": 4,
            "Table 12": 4,
            "Table 18": 3,
            "Table 31": 5,
        }
        table_df = table_df.iloc[drop_row_lookup.get(sheet_name) :].reset_index(
            drop=True
        )
        table_df.columns = table_df.iloc[0]
        table_df = table_df.drop(index=0).reset_index(drop=True)

        category_2_name = table_df.columns.to_list()[1]
        table_df = (
            melt_dataframe(
                dataframe=table_df,
                table_name=sheet_name,
                id_vars=[category_1_name, category_2_name],
                value_name="value",
                var_name="variable",
            )
            .pipe(
                remove_special_chars,
                table_name=sheet_name,
                column_to_clean="value",
                char_to_remove=r"\^",  # ^ Numbers based on a low sample count (between 10 and less than 30) have been marked with ^. Inferences should not be made on this figure.
            )
            .pipe(
                remove_special_chars,
                table_name=sheet_name,
                column_to_clean="value",
                char_to_remove=r"\,",
            )
            .pipe(
                replace_value_in_column,
                table_name=sheet_name,
                column_name="value",
                replaced_value="*",  # * Numbers based on very low sample count (less than 10) have been hidden with *.
                replacement_value=0.0,
            )
            .pipe(
                assert_dtypes,
                table_name=sheet_name,
                expected_dtypes=table_dict.get("dtypes"),
            )
        )

    # Layout type: Single table with type breakdown
    if sheet_name in ["Table 13", "Table 16"]:
        category_1_name = table_df.columns.to_list()[0]
        category_2_name = table_df.columns.to_list()[1]
        table_df = (
            drop_nan_rows(dataframe=table_df, table_name=sheet_name, threshold=3)
            .reset_index(drop=True)
            .pipe(
                forward_fill_nan_by_column,
                table_name=sheet_name,
                column_name=category_1_name,
            )
            .pipe(
                melt_dataframe,
                table_name=sheet_name,
                id_vars=[category_1_name, category_2_name],
                value_name="value",
                var_name="variable",
            )
            .pipe(
                remove_special_chars,
                table_name=sheet_name,
                column_to_clean="value",
                char_to_remove=r"\^",  # ^ Numbers based on a low sample count (between 10 and less than 30) have been marked with ^. Inferences should not be made on this figure.
            )
            .pipe(
                remove_special_chars,
                table_name=sheet_name,
                column_to_clean="value",
                char_to_remove=r"\,",
            )
            .pipe(
                replace_value_in_column,
                table_name=sheet_name,
                column_name="value",
                replaced_value="*",  # * Numbers based on very low sample count (less than 10) have been hidden with *.
                replacement_value=0.0,
            )
            .pipe(
                assert_dtypes,
                table_name=sheet_name,
                expected_dtypes=table_dict.get("dtypes"),
            )
        )

    # Layout type: Two tables with total and type breakdown in second table
    # Table 15 treated differently due to [Note A]/[Note B] difference in first column header
    if sheet_name in ["Table 15"]:
        category_1_name = table_df.columns.to_list()[0]
        table_df = (
            drop_nan_rows(dataframe=table_df, table_name=sheet_name, threshold=2)
            .reset_index(drop=True)
            .pipe(
                forward_fill_nan_by_column,
                table_name=sheet_name,
                column_name=category_1_name,
            )
        )

        # Read full table only
        table_df = table_df.iloc[6:].reset_index(drop=True)
        table_df.columns = table_df.iloc[0]
        table_df = table_df.drop(index=0).reset_index(drop=True)

        category_1_name = table_df.columns.to_list()[0]
        category_2_name = table_df.columns.to_list()[1]

        table_df = (
            melt_dataframe(
                dataframe=table_df,
                table_name=sheet_name,
                id_vars=[category_1_name, category_2_name],
                value_name="value",
                var_name="variable",
            )
            .pipe(
                remove_special_chars,
                table_name=sheet_name,
                column_to_clean="value",
                char_to_remove=r"\^",  # ^ Numbers based on a low sample count (between 10 and less than 30) have been marked with ^. Inferences should not be made on this figure.
            )
            .pipe(
                remove_special_chars,
                table_name=sheet_name,
                column_to_clean="value",
                char_to_remove=r"\,",
            )
            .pipe(
                replace_value_in_column,
                table_name=sheet_name,
                column_name="value",
                replaced_value="*",  # * Numbers based on very low sample count (less than 10) have been hidden with *.
                replacement_value=0.0,
            )
            .pipe(
                rename_columns,
                table_name=sheet_name,
                column_map={"Wall insulation\n[Note B]": "Wall insulation [Note B]"},
            )
            .pipe(
                assert_dtypes,
                table_name=sheet_name,
                expected_dtypes=table_dict.get("dtypes"),
            )
        )

    # Layout type: Three tables with total and most dissagregated type breakdown in third table
    if sheet_name in ["Table 19"]:
        category_1_name = table_df.columns.to_list()[0]
        table_df = (
            drop_nan_rows(dataframe=table_df, table_name=sheet_name, threshold=2)
            .reset_index(drop=True)
            .pipe(
                forward_fill_nan_by_column,
                table_name=sheet_name,
                column_name=category_1_name,
            )
        )

        # Read full table only
        table_df = table_df.iloc[21:].reset_index(drop=True)
        table_df.columns = table_df.iloc[0]
        table_df = table_df.drop(index=0).reset_index(drop=True)

        category_1_name = table_df.columns.to_list()[0]
        category_2_name = table_df.columns.to_list()[1]

        table_df = (
            melt_dataframe(
                dataframe=table_df,
                table_name=sheet_name,
                id_vars=[category_1_name, category_2_name],
                value_name="value",
                var_name="variable",
            )
            .pipe(
                remove_special_chars,
                table_name=sheet_name,
                column_to_clean="value",
                char_to_remove=r"\^",  # ^ Numbers based on a low sample count (between 10 and less than 30) have been marked with ^. Inferences should not be made on this figure.
            )
            .pipe(
                remove_special_chars,
                table_name=sheet_name,
                column_to_clean="value",
                char_to_remove=r"\,",
            )
            .pipe(
                replace_value_in_column,
                table_name=sheet_name,
                column_name="value",
                replaced_value="*",  # * Numbers based on very low sample count (less than 10) have been hidden with *.
                replacement_value=0.0,
            )
            .pipe(
                rename_columns,
                table_name=sheet_name,
                column_map={"Tenure\n[Note A]": "Tenure [Note A]"},
            )
            .pipe(
                replace_value_in_column,
                table_name=sheet_name,
                column_name="Tenure [Note A]",
                replaced_value="Housing association",
                replacement_value="Housing association (Registered Social Landlords)",  # Note A
            )
            .pipe(
                assert_dtypes,
                table_name=sheet_name,
                expected_dtypes=table_dict.get("dtypes"),
            )
        )

    # Layout type: Single table with type breakdown
    # Table 29 treated differently due to label formatting difference in first column
    if sheet_name in ["Table 29"]:
        category_1_name = table_df.columns.to_list()[0]
        category_2_name = table_df.columns.to_list()[1]
        table_df = (
            drop_nan_rows(dataframe=table_df, table_name=sheet_name, threshold=3)
            .reset_index(drop=True)
            .pipe(
                forward_backward_fill_nan_by_column,
                table_name=sheet_name,
                column_name=category_1_name,
                limit=1,
            )
            .pipe(
                melt_dataframe,
                table_name=sheet_name,
                id_vars=[category_1_name, category_2_name],
                value_name="value",
                var_name="variable",
            )
            .pipe(
                remove_special_chars,
                table_name=sheet_name,
                column_to_clean="value",
                char_to_remove=r"\^",  # ^ Numbers based on a low sample count (between 10 and less than 30) have been marked with ^. Inferences should not be made on this figure.
            )
            .pipe(
                remove_special_chars,
                table_name=sheet_name,
                column_to_clean="value",
                char_to_remove=r"\,",
            )
            .pipe(
                replace_value_in_column,
                table_name=sheet_name,
                column_name="value",
                replaced_value="*",  # * Numbers based on very low sample count (less than 10) have been hidden with *.
                replacement_value=0.0,
            )
            .pipe(
                assert_dtypes,
                table_name=sheet_name,
                expected_dtypes=table_dict.get("dtypes"),
            )
        )

    # Layout type: Two single breakdown tables, both to be read
    if sheet_name in ["Table 34"]:
        category_1_name = table_df.columns.to_list()[0]
        table_df = drop_nan_rows(
            dataframe=table_df, table_name=sheet_name, threshold=2
        ).reset_index(drop=True)

        table_df["category"] = [
            "In receipt of benefits - excluding disability benefits",
            "In receipt of benefits - excluding disability benefits",
            "All households",
            "In receipt of benefits - including disability benefits",
            "In receipt of benefits - including disability benefits",
            "In receipt of benefits - including disability benefits",
            "All households",
        ]

        row_to_drop = (
            table_df[category_1_name]
            .to_list()
            .index("In receipt of benefits - including disability benefits")
        )
        table_df = drop_row(
            dataframe=table_df, table_name=sheet_name, index_list_to_drop=row_to_drop
        )

        table_df = (
            melt_dataframe(
                dataframe=table_df,
                table_name=sheet_name,
                id_vars=[
                    "category",
                    category_1_name,
                ],
                value_name="value",
            )
            .pipe(drop_nan_rows, table_name=sheet_name, threshold=4)
            .reset_index(drop=True)
            .pipe(
                rename_columns,
                table_name=sheet_name,
                column_map={category_1_name: "group"},
            )
            .pipe(
                drop_duplicate_rows,
                table_name=sheet_name,
            )
            .pipe(
                remove_special_chars,
                table_name=sheet_name,
                column_to_clean="value",
                char_to_remove=r"\^",  # ^ Numbers based on a low sample count (between 10 and less than 30) have been marked with ^. Inferences should not be made on this figure.
            )
            .pipe(
                replace_value_in_column,
                table_name=sheet_name,
                column_name="value",
                replaced_value="*",  # * Numbers based on very low sample count (less than 10) have been hidden with *.
                replacement_value=0.0,
            )
            .pipe(
                assert_dtypes,
                table_name=sheet_name,
                expected_dtypes=table_dict.get("dtypes"),
            )
        )

    transformed_tables[sheet_name] = table_df

# %%
# Create table to name lookup for subset id
table_name_lookup = {
    "Table 1": " by all households and vulnerable households",
    "Table 2": " by quadrant of lileee matrix",
    "Table 3": " by fuel poverty energy efficiency rating fpeer",
    "Table 5": " by rurality and fpeer",
    "Table 6": " by region",
    "Table 7": " by dwelling type",
    "Table 8": " by age of dwelling",
    "Table 9": " by floor area",
    "Table 10": " by gas grid connection and fpeer",
    "Table 11": " by central heating and fpeer",
    "Table 12": " by main fuel type and fpeer",
    "Table 13": " by central heating and fuel",
    "Table 14": " by boiler type",
    "Table 15": " by wall insulation and fpeer",
    "Table 16": " by wall type and gas grid connection",
    "Table 17": " by loft insulation",
    "Table 18": " by renewable heat technology and fpeer",
    "Table 19": " by tenure and fpeer",
    "Table 20": " by housing sector",
    "Table 24": " by household size",
    "Table 29": " by tenure and vulnerability",
    "Table 31": " by income decile and fpeer",
    "Table 32": " by gas payment method",
    "Table 33": " by electricity payment method",
    "Table 34": " by benefit receipt",
    "Table 35": " by eco4 hthg eligibility",
}

# %%
# Load to s3 with transformation log as metadata
s3_file_path = []
for name, dataframe in transformed_tables.items():

    metadata = {}
    with open(log_file_path, "r") as log_file:
        i = 1
        for line in log_file:
            if f"'{name}'" in line:
                metadata[f"bronze_to_silver_transformation_{i}"] = line
                i = i + 1

    # Encode metadata to parquet metadata via DataFrame attrs property
    dataframe.attrs = metadata

    file_path = save_to_s3_silver(
        dataframe=dataframe,
        dataset_name=dataset_name,
        main_file_dict=latest_version,
        file_url_index=0,
        subset_id=(name + table_name_lookup.get(name)).replace(" ", "_"),
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
