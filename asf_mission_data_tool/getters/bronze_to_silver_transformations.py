import re
import pandas as pd
import logging

from sklego.pandas_utils import log_step
from typing import Dict, Optional, List, Any

"""
Set up logger
"""

logger = logging.getLogger(__name__)  # Module-level logger


def set_logger(file_path: str):
    """Sets up the logger to write logs to a file."""
    logger.setLevel(logging.DEBUG)
    file_handler = logging.FileHandler(file_path, mode="w")
    file_handler.setLevel(logging.DEBUG)
    fmt_file = "%(asctime)s - %(levelname)s - %(message)s"
    file_formatter = logging.Formatter(fmt_file)
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)


def log_decorator(fn):
    """A decorator that applies the log_step decorator to the given function with specific logging options."""
    return log_step(time_taken=False, names=True, dtypes=True, print_fn=logger.info)(fn)


"""
Reshaping
"""


@log_decorator
def melt_dataframe(
    dataframe: pd.DataFrame,
    table_name: str,
    id_vars: List,
    value_name: str,
    var_name: Optional[str] = None,
    value_vars: Optional[List] = None,
) -> pd.DataFrame:
    """
    Transforms a given dataframe from wide format to long format.

    Parameters
    ----------
    dataframe : pd.DataFrame
        _Dataframe to be melted.
    table_name : str
        Name of table, corresponding to the sheet name in the source Excel file (used as an identifier for log statements).
    id_vars : List
        List of columns to keep fixed as identifiers.
    var_name : str
        Name of new column that will store variable names.
    value_name : str
        Name of new column that will store values.

    Returns
    -------
    pd.DataFrame
        Transformed dataframe in long format.

    Raises
    ------
    KeyError
        If any of `id_vars` or columns to be melted do not exist in the given dataframe.
    """
    try:
        if value_vars:
            dataframe_long = dataframe.melt(
                id_vars=id_vars, value_vars=value_vars, value_name=value_name
            )
        else:
            dataframe_long = dataframe.melt(
                id_vars=id_vars, var_name=var_name, value_name=value_name
            )
    except KeyError as e:
        raise KeyError(
            f"One of the specified 'id_vars' or columns to melt does not exist in {table_name}: {e}"
        )
    return dataframe_long


"""
Final checks and assertions
"""


@log_decorator
def check_missing_values(
    dataframe: pd.DataFrame,
    table_name: str,
) -> pd.DataFrame:
    """Checks for missing values in a given dataframe and raises an error if any are found.

    Parameters
    ----------
    dataframe : pd.DataFrame
        Dataframe to check for missing values.
    table_name : str
        Name of table, corresponding to the sheet name in the source Excel file (used as an identifier for log statements).

    Returns
    -------
    pd.DataFrame: Original dataframe if no missing values are found.


    Raises
    ------
    ValueError
        If dataframe contains missing (null) values.
    """

    if dataframe.isnull().values.any():
        raise ValueError(f"Error: {table_name} contains missing values.")
    return dataframe


@log_decorator
def assert_dtypes(
    dataframe: pd.DataFrame, table_name: str, expected_dtypes: Dict
) -> pd.DataFrame:
    """
    Ensures that specified columns in a given dataframe have the expected data types.

    This function checks whether each column in `expected_dtypes` exists in the dataframe and whether its data type matches the expected type.
    If a column in missing, a `KeyError` is raised. If a column's data type does not match, an attempt is made to cast it to the expected type.
    If casting fails, a `ValueError` is raised.

    Parameters
    ----------
    dataframe : pd.DataFrame
        Dataframe to check and enforce data types.
    table_name : str
        Name of table, corresponding to the sheet name in the source Excel file (used as an identifier for log statements).
    expected_dtypes : Dict
        Dictionary mapping column names to expected data types.

    Returns
    -------
    pd.DataFrame: Dataframe with enforced data types.

    Raises
    ------
    KeyError
        If a column expected in `expected_dtypes` is missing from dataframe.
    ValueError
        If a column cannot be cast to the expected data type.
    """

    for col, dtype in expected_dtypes.items():

        if col not in dataframe.columns:
            raise KeyError(
                (
                    f"Expected column '{col}' not found in the dataframe for table '{table_name}'"
                )
            )
        if dataframe[col].dtype != dtype:
            try:
                dataframe[col] = dataframe[col].astype(dtype)
            except ValueError as e:
                raise ValueError(
                    f"Unable to cast column '{col}' to type '{dtype}' in table '{table_name}'"
                ) from e
    return dataframe


"""
Dropping rows and columns
"""


@log_decorator
def drop_duplicate_rows(
    dataframe: pd.DataFrame,
    table_name: str,
    consider_columns: list[str] = None,
    keep: str = "first",
) -> pd.DataFrame:
    """
    Drops duplicated rows, keeps first occurrence.

    Parameters
    ----------
    df : pd.DataFrame
        Dataframe from which rows will be dropped.
    table_name : str
        Name of table, corresponding to the sheet name in the source Excel file (used as an identifier for log statements).
    consider_columns : list[str]
        A list of columns to consider for identifying duplicates, default is all columns.

    Returns
    -------
    pd.DataFrame
        Dataframe with duplicate rows dropped.
    """
    dataframe = dataframe.drop_duplicates(subset=consider_columns, keep=keep)
    return dataframe


@log_decorator
def drop_row(
    dataframe: pd.DataFrame, table_name: str, index_list_to_drop: list
) -> pd.DataFrame:
    """
    Drops specified rows from a given dataframe by their index.

    This function removes the rows from the dataframe that are specified in `index_list_to_drop`.
    After dropping the rows, the index is reset.

    Parameters
    ----------
    df : pd.DataFrame
        Dataframe from which rows will be dropped.
    table_name : str
        Name of table, corresponding to the sheet name in the source Excel file (used as an identifier for log statements).
    index_list_to_drop : list
        A list of indices (row numbers) to drop from the dataframe.

    Returns
    -------
    pd.DataFrame
        Dataframe with specified rows dropped and index reset.
    """
    dataframe = dataframe.drop(index_list_to_drop).reset_index(drop=True)
    return dataframe


@log_decorator
def drop_nan_rows(
    dataframe: pd.DataFrame, table_name: str, threshold: int
) -> pd.DataFrame:
    """Drops rows from a given dataframe that contain NaN values; a row will only be dropped
    if it contains over a threshold number of non-NaN values, e.g. threshold of 2 will ensure
    that kept rows will have at least 2 non-NaN values.

    Parameters
    ----------
    dataframe : pd.DataFrame
        Dataframe from which rows with NaN values will be dropped.
    table_name : str
        Name of table, corresponding to the sheet name in the source Excel file (used as an identifier for log statements).
    threshold : int
        Require that many non-NA values.

    Returns
    -------
    pd.DataFrame
        _description_
    """
    dataframe = dataframe.dropna(axis="rows", thresh=threshold)
    return dataframe


@log_decorator
def drop_nan_columns(dataframe: pd.DataFrame, table_name: str) -> pd.DataFrame:
    """Drops columns from a given dataframe that contain only NaN values.

    Parameters
    ----------
    dataframe : pd.DataFrame
        Dataframe from which columns with all NaN values will be dropped.
    table_name : str
        Name of table, corresponding to the sheet name in the source Excel file (used as an identifier for log statements).

    Returns
    -------
    pd.DataFrame
        Dataframe with columns containing only NaN values removed.
    """
    dataframe = dataframe.dropna(axis="columns", how="all")
    return dataframe


@log_decorator
def drop_header_field_rows(
    dataframe: pd.DataFrame, table_name: str, column_list: list
) -> pd.DataFrame:
    """
    Removes rows from a DataFrame where column names appear as repeated values.

    This function is useful for cleaning data where header rows are
    included multiple times within the table. It checks if any value in the
    specified columns matches the column name and removes such rows.

    Parameters
    ----------
    dataframe : pd.DataFrame
        Dataframe to be cleaned.
    table_name : str
        Name of table, corresponding to the sheet name in the source Excel file (used as an identifier for log statements).
    column_list : list
        List of column names to check for repeated header rows.

    Returns
    -------
    pd.DataFrame
        Cleaned dataframe with repeated header rows removed.
    """
    masks = []
    for column_name in column_list:
        masks.append(dataframe[column_name] == column_name)
    return dataframe[~pd.concat(masks, axis=1).any(axis=1)]


@log_decorator
def filter_dataframe_on_column(
    dataframe: pd.DataFrame, table_name: str, column_name: str, filter: Any
) -> pd.DataFrame:
    """
    Filters dataframe based on specific column and value.

    This function filters the dataframe by keeping only the rows where value in the specified column matches
    the provided filter value.

    Parameters
    ----------
    dataframe : pd.DataFrame
        Dataframe to be filtered.
    table_name : str
        Name of table, corresponding to the sheet name in the source Excel file (used as an identifier for log statements).
    column_name : str
        Name of column to filter on.
    filter : Any
        Value to filter by. Rows where the column value matches this filter will be retained.

    Returns
    -------
    pd.DataFrame
        Dataframe containing only the rows where the column value matches the filter.
    """
    return dataframe[dataframe[column_name] == filter]


"""
Addressing missing values
"""


@log_decorator
def forward_fill_nan_by_row(
    dataframe: pd.DataFrame,
    table_name: str,
    row_index: int = 0,
    fill_header: bool = False,
) -> pd.DataFrame:
    """
    Performs forward fill on NaN values row-wise for a specified row in a given dataframe.
    Optionally, fills missing values in the header row (column names).

    This function replaces NaN values in a specific row using forward fill,
    which propagates the last valid value forward along the row. If NaNs
    appear at the beginning of the row, they will remain unchanged.

    Parameters
    ----------
    dataframe : pd.DataFrame
        Dataframe containing data to be forward-filled row-wise.
    table_name : str
        Name of table, corresponding to the sheet name in the source Excel file
        (used as an identifier for log statements).
    row_index : int
        Index of the row to apply forward fill. Ignored if `fill_header` is True.
    fill_header : bool, optional
        If True, forward fill missing values in the column headers instead of a specific row.

    Returns
    -------
    pd.DataFrame
        Dataframe with forward-filled values for the specified row or headers.

    Raises
    ------
    KeyError
        If the dataframe is empty or the row index is out of bounds.

    Example
    -------
    df = pd.DataFrame({
        "A": [1, None, 3],
        "B": [None, 2, None],
        "C": [4, None, 6]
    })
    forward_fill_nan_by_row(df, "ExampleTable", 1)

    # Result:
    #      A    B  C
    # 0  1.0  1.0  4
    # 1  NaN  2.0  2
    # 2  3.0  3.0  6

    # Forward filling headers:
    df.columns = [None, "B", None]
    df = forward_fill_nan_by_row(df, "ExampleTable", 0, fill_header=True)
    # Result:
    # Columns: [None, "B", "B"]
    """

    if fill_header:
        dataframe.columns = dataframe.columns.to_series().ffill()
    else:
        if row_index not in dataframe.index:
            raise KeyError(f"Row index {row_index} not found in table '{table_name}'.")
        dataframe.loc[row_index] = dataframe.loc[row_index].ffill()

    return dataframe


@log_decorator
def forward_fill_nan_by_column(
    dataframe: pd.DataFrame, table_name: str, column_name: str
) -> pd.DataFrame:
    """
    Performs forward fill on NaN values in a specified column of a given dataframe.

    this function replaces NaN values in the given column using forward fill which propagates the last valid value forward. If NaNs appear at the beginning of the column, they will remain unchanged.

    Parameters
    ----------
    dataframe : pd.DataFrame
        Dataframe containing column to be filled.
    table_name : str
        Name of table, corresponding to the sheet name in the source Excel file (used as an identifier for log statements).
    column_name : str
        Name of column where NaN values should be forward-filled.

    Returns
    -------
    pd.DataFrame
        Dataframe with forward-filled values in specified column.

    Raises
    ------
    KeyError
        If any of `id_vars` or columns to be melted do not exist in the given dataframe.

    """
    if column_name not in dataframe.columns:
        raise KeyError(f"Column '{column_name}' not found in table '{table_name}'.")

    dataframe[column_name] = dataframe[column_name].ffill()
    return dataframe


@log_decorator
def forward_backward_fill_nan_by_column(
    dataframe: pd.DataFrame, table_name: str, column_name: str, limit: int
) -> pd.DataFrame:
    """Performs forward and backward fill of NaN values in specified column of given dataframe,
    within a maximum number of consecutive NaN values (specified by limit).

    Parameters
    ----------
    dataframe : pd.DataFrame
        Dataframe containing column to be filled.
    table_name : str
        Name of table, corresponding to the sheet name in the source Excel file (used as an identifier for log statements).
    column_name : str
        Name of column where NaN values to be filled.
    limit : int
        Maximum number of consecutive NaN values to be filled.

    Returns
    -------
    pd.DataFrame
        Dataframe with filled values in specified column.

    Raises
    ------
    KeyError
        If any of `id_vars` or columns to be melted do not exist in the given dataframe.
    """

    if column_name not in dataframe.columns:
        raise KeyError(f"Column '{column_name}' not found in table '{table_name}'.")

    dataframe[column_name] = (
        dataframe[column_name].ffill(limit=limit).bfill(limit=limit)
    )
    return dataframe


@log_decorator
def replace_nan_in_column(
    dataframe: pd.DataFrame, table_name: str, column_name: str, value: Any
) -> pd.DataFrame:
    """
    Replaces NaN values in a specified column with a given value.

    Parameters
    ----------
    dataframe : pd.DataFrame
        Dataframe with column to be modified.
    table_name : str
        Name of table, corresponding to the sheet name in the source Excel file (used as an identifier for log statements).
    column_name : str
        Name of column where NaN values will be replaced.
    value : Any
        Value to replace NaN values with.

    Returns
    -------
    pd.DataFrame
        Dataframe with NaN values in specified column replaced with the given value.
    """
    dataframe[column_name] = dataframe[column_name].mask(
        dataframe[column_name].isna(), value
    )
    return dataframe


"""
Modifying column names or values
"""


@log_decorator
def convert_year_column_to_int(
    dataframe: pd.DataFrame, table_name: str, year_column_name: str
) -> pd.DataFrame:
    """
    Converts a specified year column in the Dataframe to the integer data type.

    This function attempts to convert the specified column to the integer data type (`pd.Int64Dtype()`),
    which allows for handling of missing values as `NaN` while keeping the column's type as integer.

    Parameters
    ----------
    dataframe : pd.DataFrame
        Dataframe containing the column to be converted.
    table_name : str
        Name of table, corresponding to the sheet name in the source Excel file (used as an identifier for log statements).
    year_column_name : str
        Name of column containing values to be converted.

    Returns
    -------
    pd.DataFrame
        Dataframe with specified column converted to integer type.
    """
    dataframe[year_column_name] = dataframe[year_column_name].astype(pd.Int64Dtype())
    return dataframe


@log_decorator
def rename_columns(
    dataframe: pd.DataFrame, table_name: str, column_map: Dict
) -> pd.DataFrame:
    """
    Renames columns in given dataframe based on mapping provided in `column_map`, where keys are current column
    names and values are new names for those columns.

    Parameters
    ----------
    dataframe : pd.DataFrame
        Dataframe with columns to be renamed.
    table_name : str
        Name of table, corresponding to the sheet name in the source Excel file (used as an identifier for log statements).
    column_map : Dict
        Dictionary where keys are current column names and values are the new names.

    Returns
    -------
    pd.DataFrame
        Dataframe with columns renamed as per provided mapping.
    """
    dataframe = dataframe.rename(columns=column_map)
    return dataframe


@log_decorator
def convert_to_nan(
    dataframe: pd.DataFrame, column_name: str, to_replace: str, table_name: str
) -> pd.DataFrame:
    """
    Converts specified values in a column to NaN.

    This function replaces occurrences of the specified `to_replace` value
    in the given column with `NaN` and converts the column to a numeric type.
    Any invalid conversions are coerced to NaN.

    Parameters
    ----------
    df : pd.DataFrame
        Dataframe containing the column to modify.
    column_name : str
        Name of column where values should be replaced.
    to_replace : str
        Value in column to replace with NaN.
    table_name : str
        Name of table, corresponding to the sheet name in the source Excel file (used as an identifier for log statements).

    Returns
    -------
    pd.DataFrame
        Dataframe with specified values replaced by NaN, and column converted to numeric type.
    """
    dataframe[column_name] = pd.to_numeric(
        dataframe[column_name].replace(to_replace, None), errors="coerce"
    )
    return dataframe


@log_decorator
def extract_substring_in_column(
    dataframe: pd.DataFrame,
    table_name: str,
    separator: str,
    column_name: str,
    segment_to_extract: int,
) -> pd.DataFrame:
    """
    Extracts a substring from a column based on a specified separator.

    This function splits the values in the specified column at the provided `separator` and extracts the substring
    at the position specified by `segment_to_extract`. It updates the column with the extracted substrings.

    Parameters
    ----------
    dataframe : pd.DataFrame
        Dataframe containing the column to process.
    table_name : str
        Name of table, corresponding to the sheet name in the source Excel file (used as an identifier for log statements).
    separator : str
        Separator used to split the values in the column.
    column_name : str
        Name of column where substring will be extracted from.
    segment_to_extract : int
        Index of segment to extract, where 0 is the first.

    Returns
    -------
    pd.DataFrame
        Dataframe with specified column updated with extracted substrings.
    """
    dataframe[column_name] = dataframe[column_name].apply(
        lambda x: x.split(separator)[segment_to_extract]
    )
    return dataframe


@log_decorator
def replace_value_in_column(
    dataframe: pd.DataFrame,
    table_name: str,
    column_name: str,
    replaced_value: Any,
    replacement_value: Any,
) -> pd.DataFrame:
    """
    Replaces a specified value in a column with a new value.

    Parameters
    ----------
    dataframe : pd.DataFrame
        Dataframe with column to be modified.
    table_name : str
        Name of table, corresponding to the sheet name in the source Excel file (used as an identifier for log statements).
    column_name : str
        Name of column with values to be replaced.
    replaced_value : Any
        Value to be replaced.
    Replacement_value : Any
        Value to replace the `replaced_value`.

    Returns
    -------
    pd.DataFrame
        Dataframe with replaced values in specified column.

    """
    dataframe[column_name] = dataframe[column_name].mask(
        dataframe[column_name] == replaced_value, replacement_value
    )
    return dataframe


@log_decorator
def forward_fill_unnamed_columns(
    dataframe: pd.DataFrame, table_name: str
) -> pd.DataFrame:
    dataframe.columns = (
        dataframe.columns.to_series()
        .mask(lambda x: x.str.startswith("Unnamed"))
        .ffill()
    )
    return dataframe


@log_decorator
def remove_line_break_chars(
    dataframe: pd.DataFrame, table_name: str, column_to_clean: str
) -> pd.DataFrame:
    dataframe[column_to_clean] = dataframe[column_to_clean].replace(
        r"\n", " ", regex=True
    )
    return dataframe


@log_decorator
def remove_special_chars(
    dataframe: pd.DataFrame, table_name: str, column_to_clean: str, char_to_remove: str
) -> pd.DataFrame:
    dataframe[column_to_clean] = dataframe[column_to_clean].replace(
        char_to_remove, "", regex=True
    )
    return dataframe


@log_decorator
def trim_whitespace(dataframe: pd.DataFrame, table_name: str) -> pd.DataFrame:
    dataframe = dataframe.map(lambda x: x.strip() if isinstance(x, str) else x)
    return dataframe


"""
Adding new columns or rows
"""


@log_decorator
def add_constant_column(
    dataframe: pd.DataFrame, table_name: str, column_name: str, constant_value: Any
) -> pd.DataFrame:
    """
    Adds a new column with a constant value to a given dataframe.

    Parameters
    ----------
    dataframe : pd.DataFrame
        Dataframe to which the new column will be added.
    table_name : str
        Name of table, corresponding to the sheet name in the source Excel file (used as an identifier for log statements).
    column_name : str
        Name of new column to be added.
    constant_value : Any
        Value to assign to the new column for all rows.

    Returns
    -------
    pd.DataFrame
        Dataframe with new column added.
    """
    dataframe[column_name] = constant_value
    return dataframe


"""
Specific to DESNZ Heat Pump Deployment Quarterly Statistics
"""


@log_decorator
def standardise_column_names(
    dataframe: pd.DataFrame,
    table_name: str,
) -> pd.DataFrame:
    """
    Standardises column names across all tables by applying specific formatting rules.

    This function checks the column names of the given dataframe and standardises them according to predefined rules
    for different tables. It performs checks for specific substrings in the column names and applies transformations
    such as removing newline characters and replacing certain substrings.

    Parameters
    ----------
    dataframe : pd.DataFrame
        Dataframe with column names to be standardised.
    table_name : str
        Name of table, corresponding to the sheet name in the source Excel file (used as an identifier for log statements).

    Returns
    -------
    pd.DataFrame
        Dataframe with standardised column names.

    Raises
    ------
    ValueError
        If column names do not meet the expected format for the given table name.
    """

    if table_name == "Table 1.1":

        # Check if format of column names are as expected by this function
        if not dataframe.columns.str.contains("\n").any():
            raise ValueError(
                "Expected newline characters in column names for Table 1.1"
            )
        if not dataframe.columns.str.contains("Of which:").any():
            raise ValueError("Expected 'Of which:' in column names for Table 1.1")
        if not dataframe.columns.str.contains(
            "Total: Government-supported heat pump installations"
        ).any():
            raise ValueError(
                "Expected 'Total: Government-supported heat pump installations' in column names for Table 1.1"
            )

        dataframe.columns = (
            dataframe.columns.str.strip()
            .str.replace("\n", "", regex=True)
            .str.replace("Of which:", "", regex=False)
            .str.replace(
                "Total: Government-supported heat pump installations",
                "All schemes",
                regex=False,
            )
        )

    elif table_name == "Table 1.2":

        # Check if format of column names are as expected by this function
        if not dataframe.columns.str.contains("\n").any():
            raise ValueError(
                "Expected newline characters in column names for Table 1.2"
            )
        if not dataframe.columns.str.contains(
            "Government-supported heat pump installations:"
        ).any():
            raise ValueError(
                "Expected 'Government-supported heat pump installations:' in column names for Table 1.2"
            )

        dataframe.columns = (
            dataframe.columns.str.strip()
            .str.replace("\n", "", regex=True)
            .str.replace(
                "Government-supported heat pump installations:", "", regex=False
            )
            .str.capitalize()
        )
    elif table_name == "Table 1.3":
        pass
    else:
        raise ValueError(
            "Sheet name does not correspond to a table in the dataset file."
        )
    return dataframe


@log_decorator
def expand_quarter_string(dataframe: pd.DataFrame, table_name: str) -> pd.DataFrame:
    """
    Converts a quarter string (e.g., "2018 Q1: January to March") into structured components:
    quarter, year, start date, and end date. Tailored to the DESNZ Heat Pump Deployment Quarterly Statistics data tables.

    Parameters
    ----------
    quarter_str : str
        A string representing a year and quarter in the format "YYYY QX" (e.g., "2018 Q1") or "Unknown".
    table_name : str
        Name of table, corresponding to the sheet name in the source Excel file (used as an identifier for log statements).

    Returns
    -------
    tuple[str, str, Optional[pd.Timestamp], Optional[pd.Timestamp]]
        - Quarter (e.g., "Q1", "Q2", "Q3", "Q4") or "Unknown".
        - Year as a string or "Unknown".
        - Start date of the quarter as a pandas Timestamp (or None if "Unknown").
        - End date of the quarter as a pandas Timestamp (or None if "Unknown").
    """

    def convert_quarter_string(
        quarter_str: str,
    ) -> tuple[str, str, Optional[pd.Timestamp], Optional[pd.Timestamp]]:

        # Validate that input string is in expected format
        pattern = r"^\d{4} Q[1-4]: .+"
        if quarter_str != "Unknown" and not re.match(pattern, quarter_str):
            raise ValueError(
                f"Invalid quarter format: {quarter_str}. Expected format 'YYYY QX: <Month range of quarter>'."
            )

        # Handle Unknown cases
        if "Unknown" in quarter_str:
            return "Unknown", pd.NA, None, None

        year, quarter_number = quarter_str.split(" Q")
        year = int(year)
        quarter = int(quarter_number[0])

        start_dates = {
            1: f"{year}-01-01",
            2: f"{year}-04-01",
            3: f"{year}-07-01",
            4: f"{year}-10-01",
        }

        end_dates = {
            1: f"{year}-03-31",
            2: f"{year}-06-30",
            3: f"{year}-09-30",
            4: f"{year}-12-31",
        }

        return (
            f"Q{quarter}",
            year,
            pd.to_datetime(start_dates[quarter]),
            pd.to_datetime(end_dates[quarter]),
        )

    dataframe[
        ["Installation quarter [note 4]", "Year", "Start of quarter", "End of quarter"]
    ] = dataframe["Installation quarter [note 4]"].apply(
        lambda x: pd.Series(convert_quarter_string(x))
    )
    dataframe["Year"] = dataframe["Year"].astype(pd.Int64Dtype())

    return dataframe


"""
Specific to Energy price cap levels
"""


def _extract_payment_method_tables(
    df: pd.DataFrame, payment_method: str
) -> pd.DataFrame:
    """
    Extracts a specific payment method table from a master dataframe.
    Tailored to the Ofgem Annex 9 energy price cap tariff data tables.

    This function identifies and isolates a table corresponding to a specific payment method
    ("Other Payment Method", "Standard Credit", "PPM") in the dataframe. It extracts a subset of rows
    and columns based on the payment method and cleans up the table by forward filling missing column names,
    dropping fully empty rows and columns, and resetting the index.

    Parameters
    ----------
    df : pd.DataFrame
        Dataframe containing all payment method tables.
    payment_method : str
        The payment method for which the table should be extracted. Valid options are:
        - "Other Payment Method"
        - "Standard Credit"
        - "PPM"

    Returns
    -------
    pd.DataFrame
        Dataframe containing the extracted payment method table with unnecessary rows
        and columns removed, and missing column names forward-filled.
    """

    payment_method_number = {"Other Payment Method": 1, "Standard Credit": 3, "PPM": 5}

    # Get row index range
    start_index = df.index[df["Unnamed: 1"] == payment_method].tolist()[0]

    # Only dual fuel table has Total inc VAT row
    dual_fuel_header_column = df.columns[
        df.apply(
            lambda col: col.astype(str).str.contains("Dual fuel (implied)", regex=False)
        ).any()
    ][0]
    end_index = df.index[df[dual_fuel_header_column] == "Total inc VAT"].tolist()[
        payment_method_number.get(payment_method)
    ]

    # Isolate payment method tables
    payment_method_df = df.iloc[start_index : end_index + 1, :]

    # Forward fill empty column names
    pd.set_option("future.no_silent_downcasting", True)
    payment_method_df = payment_method_df.ffill(axis="columns").infer_objects(
        copy=False
    )

    # Drop fully empty rows and columns
    payment_method_df = (
        payment_method_df.dropna(axis="index", how="all")
        .dropna(axis="columns", how="all")
        .reset_index(drop=True)
    )

    return payment_method_df


def _shape_fuel_tables_to_tidy(
    payment_method_df: pd.DataFrame, payment_method: str, fuel: str
) -> pd.DataFrame:
    """
    Transforms a fuel table for a specific payment method into a tidy format.

    This function extracts columns related to the specified fuel type from the given dataframe, identifies rows corresponding to "Nil consumption"
    and "Typical consumption", and reshapes the data into a tidy format by melting the consumption and tariff component information.
    It then concatenates the reshaped data for "Nil consumption" and "Typical consumption" into a single dataframe.

    Parameters
    ----------
    payment_method_df : pd.DataFrame
        Dataframe containing data for a given payment method. Output of _extract_payment_method_tables.
    payment_method : str
        Payment method, valid options are:
        - "Other Payment Method"
        - "Standard Credit"
        - "PPM"
    fuel : str
        Fuel type of interest, valid options are:
        - "Electricity: Single-Rate Metering Arrangement"
        - "Electricity: Multi-Register Metering Arrangement"
        - "Gas"
        - "Dual fuel (implied)"

    Returns
    -------
    pd.DataFrame
        A tidy DataFrame containing the reshaped data with columns for "Payment method",
        "Fuel", "Consumption", "Tariff component", and "Price cap period".
    """

    fuel_columns = []
    for column in payment_method_df.columns:
        if payment_method_df[column].str.contains(fuel, na=False, regex=False).any():
            fuel_columns.append(column)
    fuel_df = payment_method_df[fuel_columns]

    start_nil_index = fuel_df.index[
        fuel_df[fuel_columns[0]] == "Nil consumption"
    ].tolist()[0]
    start_typical_index = fuel_df.index[
        fuel_df[fuel_columns[0]] == "Typical consumption"
    ].tolist()[0]

    # Nil consumption
    consumption_type = "Nil consumption"
    fuel_nil_df = fuel_df.iloc[
        start_nil_index : start_typical_index - 1, :
    ].reset_index(drop=True)
    fuel_nil_df.columns = fuel_nil_df.iloc[0]
    fuel_nil_df = fuel_nil_df.iloc[1:]
    fuel_nil_df = fuel_nil_df.rename(columns={consumption_type: "Tariff component"})
    fuel_nil_df["Payment method"] = payment_method
    fuel_nil_df["Consumption"] = consumption_type
    fuel_nil_df["Fuel"] = fuel

    fuel_nil_tidy = fuel_nil_df.melt(
        id_vars=["Payment method", "Fuel", "Consumption", "Tariff component"],
        var_name="Price cap period",
    )

    # Typical consumption
    consumption_type = "Typical consumption"
    fuel_typical_df = fuel_df.iloc[start_typical_index:, :].reset_index(drop=True)
    fuel_typical_df.columns = fuel_typical_df.iloc[0]
    fuel_typical_df = fuel_typical_df.iloc[1:]
    fuel_typical_df = fuel_typical_df.rename(
        columns={consumption_type: "Tariff component"}
    )
    fuel_typical_df["Payment method"] = payment_method
    fuel_typical_df["Consumption"] = consumption_type
    fuel_typical_df["Fuel"] = fuel

    fuel_typical_tidy = fuel_typical_df.melt(
        id_vars=["Payment method", "Fuel", "Consumption", "Tariff component"],
        var_name="Price cap period",
    )

    return pd.concat([fuel_nil_tidy, fuel_typical_tidy]).dropna(
        subset={"Tariff component"}
    )


@log_decorator
def convert_tariff_tables_to_tidy(
    dataframe: pd.DataFrame, table_name: str
) -> pd.DataFrame:
    """
    Converts all tariff tables into tidy format.

    Parameters
    ----------
    df : pd.DataFrame
        Raw dataframe of tariff data from reading entire sheet of Annex 9 Excel file.
    table_name : str
        Name of table, corresponding to the sheet name in the source Excel file (used as an identifier for log statements).


    Returns
    -------
    pd.DataFrame
        Tidy DataFrame containing the reshaped tariff data, with columns for "Payment method",
        "Fuel", "Consumption", "Tariff component", and "Price cap period". The data is organized
        into a long format, with each row representing a unique combination of payment method, fuel,
        consumption type, and price cap period.
    """
    payment_method_tables = {}

    for payment_method in ["Other Payment Method", "Standard Credit", "PPM"]:

        all_fuel_df = pd.DataFrame()
        for fuel in [
            "Electricity: Single-Rate Metering Arrangement",
            "Electricity: Multi-Register Metering Arrangement",
            "Gas",
            "Dual fuel (implied)",
        ]:
            fuel_df = _shape_fuel_tables_to_tidy(
                payment_method_df=_extract_payment_method_tables(
                    dataframe, payment_method
                ),
                payment_method=payment_method,
                fuel=fuel,
            )
            all_fuel_df = pd.concat([all_fuel_df, fuel_df])

        payment_method_tables[payment_method] = all_fuel_df

    return pd.concat(payment_method_tables.values(), ignore_index=True)


@log_decorator
def expand_price_cap_period(
    dataframe: pd.DataFrame, column_name: str, table_name: str
) -> pd.DataFrame:
    """
    Expands a 'Price cap period' column into two separate columns: 'Price cap period start' and 'Price cap period end'.

    This function takes the 'Price cap period' column containing a range (e.g., "Jan 2023 - Dec 2023") and splits it into
    two separate columns for the start and end date. The month names are converted to full month names (e.g., 'Jan' to 'January'),
    and the date strings are converted into `datetime` objects.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing the data, including a 'Price cap period' column that has a range of dates in string format.
    column_name : str
        Name of column containing the price cap period range.
    table_name : str
        Name of table, corresponding to the sheet name in the source Excel file (used as an identifier for log statements).

    Returns
    -------
    pd.DataFrame
        A DataFrame with two additional columns: 'Price cap period start' and 'Price cap period end'.
        These columns contain the start and end dates of the price cap period in `datetime` format.

    Raises
    ------
    ValueError
        If the 'Price cap period' column contains an invalid month format.
    """
    dataframe[["Price cap period start", "Price cap period end"]] = dataframe[
        column_name
    ].str.split(" - ", expand=True)

    def _convert_to_full_month(month_str):
        month_map = {
            "Jan": "January",
            "Feb": "February",
            "Mar": "March",
            "Apr": "April",
            "May": "May",
            "Jun": "June",
            "Jul": "July",
            "Aug": "August",
            "Sep": "September",
            "Oct": "October",
            "Nov": "November",
            "Dec": "December",
            "Sept": "September",
        }

        if month_str[:-5] in month_map.keys():
            return month_map[month_str[:-5]] + " " + month_str[-4:]
        elif month_str[:-5] in month_map.values():
            return month_str
        raise ValueError("Invalid month format")

    dataframe["Price cap period start"] = dataframe["Price cap period start"].apply(
        _convert_to_full_month
    )
    dataframe["Price cap period end"] = dataframe["Price cap period end"].apply(
        _convert_to_full_month
    )

    dataframe["Price cap period start"] = pd.to_datetime(
        dataframe["Price cap period start"], format="%B %Y"
    )
    dataframe["Price cap period end"] = pd.to_datetime(
        dataframe["Price cap period end"], format="%B %Y"
    )
    return dataframe


"""
Specific to Seventh Carbon Budget
"""


@log_decorator
def convert_sign_of_abatement(dataframe: pd.DataFrame, table_name: str) -> pd.DataFrame:
    """
    Converts the sign of the 'Value' column based on the 'Category' column.

    This function checks the 'Category' column for each row in the dataframe. If the 'Category' is
    not 'Residual emissions' (i.e. it refers to emissions abatement), it changes the sign of the value in the 'Value' column.

    Parameters
    ----------
    dataframe : pd.DataFrame
        Dataframe containing combined residual and abatement emissions data.
    table_name : str
        Name of table, corresponding to the sheet name in the source Excel file (used as an identifier for log statements).

    Returns
    -------
    pd.DataFrame
        Dataframe with adjusted 'Value' column.
    """
    dataframe["Value"] = dataframe.apply(
        lambda row: (
            row["Value"]
            if row["Category"] == "Residual emissions"
            else -abs(row["Value"])
        ),
        axis=1,
    )
    return dataframe


"""
Specific to UK Territorial Greenhouse Gas Emissions Statistics
"""


@log_decorator
def fill_sector_category_columns(
    dataframe: pd.DataFrame,
    table_name: str,
    sector_column: str,
    subsector_column: str,
    category_column: str,
) -> pd.DataFrame:

    # Fill subsector column for "total" rows
    dataframe.loc[
        dataframe[sector_column].str.contains("total", case=False, na=False),
        subsector_column,
    ] = dataframe[sector_column]

    # Fill category column for "total" rows
    dataframe.loc[
        dataframe[subsector_column].str.contains("total", case=False, na=False),
        category_column,
    ] = "Total"
    return dataframe


"""
Specific to Energy Consumption in the UK
"""


@log_decorator
def add_year_column(
    dataframe: pd.DataFrame,
    table_name: str,
    year_column_name: str,
    existing_year_column_to_duplicate: tuple,
) -> pd.DataFrame:
    dataframe[year_column_name] = dataframe[existing_year_column_to_duplicate]
    return dataframe


"""
Specific to DESNZ Public Attitudes Tracking Survey
"""


@log_decorator
def expand_wave_season_dates(
    dataframe: pd.DataFrame, table_name: str, column_name: str
) -> pd.DataFrame:

    def _get_season_dates(season: str):
        season_name, year = season.split()
        year = int(year)

        if season_name.lower() == "winter":
            start = pd.Timestamp(f"{year}-12-01")
            end = pd.Timestamp(f"{year + 1}-02-28")
        elif season_name.lower() == "spring":
            start = pd.Timestamp(f"{year}-03-01")
            end = pd.Timestamp(f"{year}-05-31")
        elif season_name.lower() == "summer":
            start = pd.Timestamp(f"{year}-06-01")
            end = pd.Timestamp(f"{year}-08-31")
        elif season_name.lower() == "autumn":
            start = pd.Timestamp(f"{year}-09-01")
            end = pd.Timestamp(f"{year}-11-30")
        else:
            raise ValueError(
                "Invalid season name. Must be one of: 'Winter', 'Spring', 'Summer', 'Autumn'"
            )

        # Return year, start and end dates
        return year, start, end

    dataframe[["wave_year", "season_start_date", "season_end_date"]] = dataframe[
        column_name
    ].apply(lambda x: pd.Series(_get_season_dates(x)))
    return dataframe


"""
Specific to English Housing Survey
"""
