import requests
import boto3
import subprocess
import toml
import logging
import yaml
import io
import pandas as pd
from copy import deepcopy
from botocore.exceptions import NoCredentialsError, ClientError
from datetime import datetime, timezone, timedelta
from bs4 import BeautifulSoup
from typing import Callable, Any, Optional, Dict
from asf_mission_data_tool import config

# Suppress information level messages from botocore
logging.getLogger("botocore.credentials").setLevel(logging.ERROR)


"""
General
"""


def get_page_url(dataset_name: str, page_link_text: str) -> str:

    # Retrieve collection page
    collection_url = config.get("dataset").get(dataset_name).get("collection_url")
    collection_response = requests.get(collection_url)

    collection_response.raise_for_status()
    if collection_response.status_code != 200:
        raise ValueError(
            f"Failed to fetch the collection page: {collection_response.status_code}"
        )

    collection_soup = BeautifulSoup(collection_response.content, "html.parser")

    # Find link to most recent release page
    page_url = None
    for a_tag in collection_soup.find_all("a", href=True):
        if page_link_text in a_tag.text:
            page_url = "https://www.gov.uk" + a_tag["href"]
            break  # first one is most recent one

    if not page_url:
        raise ValueError(f"Could not find the {page_link_text} link")

    return page_url


def get_file_url(page_url: str, file_type: str, file_text: str) -> list[str]:

    # Retrieve most recent release page
    page_response = requests.get(page_url)

    page_response.raise_for_status()
    if page_response.status_code != 200:
        raise ValueError(f"Failed to fetch the page page: {page_response.status_code}")

    page_soup = BeautifulSoup(page_response.content, "html.parser")

    # Find links to files on most recent release page
    file_url = []  # some datasets have multiple files
    for a_tag in page_soup.find_all("a", href=True):
        href = a_tag["href"]
        if href.endswith(file_type) and href.startswith("http") and file_text in href:
            file_url.append(href)

    file_url = list(dict.fromkeys(file_url))  # remove duplicates

    if not file_url:
        raise ValueError(
            f"Could not find the file link, check the link text {file_text} and file type {file_type}"
        )

    return file_url


def get_release_date(page_url: str) -> str:
    # Retrieve most recent release page
    page_response = requests.get(page_url)

    page_response.raise_for_status()
    if page_response.status_code != 200:
        raise ValueError(f"Failed to fetch the page page: {page_response.status_code}")

    page_soup = BeautifulSoup(page_response.content, "html.parser")

    # Extract release date
    published_dt = page_soup.find("dt", string="Published")
    if published_dt:
        published_dd = published_dt.find_next("dd")
        if published_dd:
            published_date = published_dd.get_text(strip=True)

    release_date = datetime.strptime(published_date, "%d %B %Y").strftime("%Y-%m-%d")

    return release_date


def add_new_version(
    dataset_name: str,
    page_url: str,
    file_url: list[str],
    release_date: str,
    filter: Optional[str] = None,
) -> None:

    # Load existing config data
    try:
        # with open("asf_mission_data_tool/config/base.yaml", "r") as file:
        with open(
            "/home/eglucas/Projects/asf_mission_data_tool/asf_mission_data_tool/config/base.yaml",
            "r",
        ) as file:
            all_existing_data = yaml.safe_load(file)
    except FileNotFoundError:
        raise FileNotFoundError("The configuration file 'base.yaml' was not found.")
    except yaml.YAMLError:
        raise ValueError("Error parsing YAML file. Please check its formatting.")

    # Retrieve entry for dataset
    dataset_specific_data = all_existing_data.get("dataset", {}).get(dataset_name, {})
    if not dataset_specific_data:
        raise KeyError(f"Dataset '{dataset_name}' not found in config file.")

    versions = dataset_specific_data.get("versions", [])
    if not versions:
        raise ValueError(f"No versions found for dataset '{dataset_name}'.")

    # check if fetched version already exists in config
    version_dates = []
    for version in versions:
        version_dates.append(version["release_date"])

    if release_date in version_dates:
        print(
            f"Dataset version {release_date} of {dataset_name} already exists in config."
        )
    else:
        # Retrieve latest version of dataset to copy schema
        if filter:
            filtered_versions = [
                version
                for version in versions
                if any(filter in url for url in version["file_url"])
            ]
            if not filtered_versions:
                raise ValueError(f"No versions found matching filter '{filter}'.")
            latest_version = max(
                filtered_versions,
                key=lambda x: datetime.strptime(x["release_date"], "%Y-%m-%d"),
            )
        else:
            latest_version = max(
                versions, key=lambda x: datetime.strptime(x["release_date"], "%Y-%m-%d")
            )

        # Create dictionary for new version to add to config
        new_version = {
            "page_url": page_url,
            "file_url": file_url,
            "release_date": release_date,
            "tables": deepcopy(
                latest_version["tables"]
            ),  # assume same structure as previous release
        }

        # Add to full dataset dictionary
        dataset_specific_data["versions"].append(new_version)

        # Write updated config
        # with open("asf_mission_data_tool/config/base.yaml", "w") as file:
        with open(
            "/home/eglucas/Projects/asf_mission_data_tool/asf_mission_data_tool/config/base.yaml",
            "w",
        ) as file:
            yaml.dump(
                all_existing_data,
                file,
                default_flow_style=False,
                sort_keys=True,
            )

        print(
            f"New version added to dataset {dataset_name} with release_date {release_date}."
        )


def get_latest_version(dataset_name: str, filter: Optional[str] = None) -> Dict:
    """Returns the latest version of a dataset instance from config.

    Parameters
    ----------
    dataset_name : str
        Name of dataset; must be a key of dataset in config/base.yaml.

    filter : Optional[str], default None
        Word to filter file URLs if only interested in gettgin the latest version of one particular category in the dataset.

    Returns
    -------
    Dict
        Dictionary with key-value pairs for dataset release date, file url and page url.
    """
    versions = config.get("dataset").get(dataset_name).get("versions")
    if not versions:
        raise ValueError(f"No versions found for dataset '{dataset_name}'.")

    if filter:
        filtered_versions = [
            version
            for version in versions
            if any(filter in url for url in version["file_url"])
        ]
        if not filtered_versions:
            raise ValueError(f"No versions found matching filter '{filter}'.")
        latest_version = max(
            filtered_versions,
            key=lambda x: datetime.strptime(x["release_date"], "%Y-%m-%d"),
        )
    else:
        latest_version = max(
            versions, key=lambda x: datetime.strptime(x["release_date"], "%Y-%m-%d")
        )
    return latest_version


def append_field_to_latest_version(
    dataset_name: str,
    s3_file_path: str,
    new_field_name: str,
    filter: Optional[str] = None,
) -> None:
    """Updates the base.yaml configuration file by appending a new field to the latest version entry of a specified dataset.
    The function reads the dataset details from an existing config file, determines the latest version based on the release date and
    updates the entry with the provided S3 file path.

    Parameters
    ----------
    dataset_name : str
        Name of dataset; must be a key of dataset in config/base.yaml.
    s3_file_path : str
        The S3 path of the silver dataset file.
    new_field_name : str
        The name of the new field to append to the base.yaml configuration file for the dataset of interest.
    filter : Optional[str], optional
        An optional filter to select specific dataset versions based on file URLs, by default None.
    """

    try:
        # Load existing config data
        with open("asf_mission_data_tool/config/base.yaml", "r") as file:
            all_existing_data = yaml.safe_load(file)
    except FileNotFoundError:
        raise FileNotFoundError("The configuration file 'base.yaml' was not found.")
    except yaml.YAMLError:
        raise ValueError("Error parsing YAML file. Please check its formatting.")

    # Retrieve entry for dataset
    dataset_specific_data = all_existing_data.get("dataset", {}).get(dataset_name, {})
    if not dataset_specific_data:
        raise KeyError(f"Dataset '{dataset_name}' not found in config file.")

    versions = dataset_specific_data.get("versions", [])
    if not versions:
        raise ValueError(f"No versions found for dataset '{dataset_name}'.")

    # Retrieve latest version of dataset
    if filter:
        filtered_versions = [
            version
            for version in versions
            if any(filter in url for url in version["file_url"])
        ]
        if not filtered_versions:
            raise ValueError(f"No versions found matching filter '{filter}'.")
        latest_version = max(
            filtered_versions,
            key=lambda x: datetime.strptime(x["release_date"], "%Y-%m-%d"),
        )
    else:
        latest_version = max(
            versions, key=lambda x: datetime.strptime(x["release_date"], "%Y-%m-%d")
        )
    latest_version[new_field_name] = s3_file_path

    # Write updated data
    with open("asf_mission_data_tool/config/base.yaml", "w") as file:
        yaml.dump(
            all_existing_data,
            file,
            default_flow_style=False,
            sort_keys=True,
        )

    print(
        f"{new_field_name} field added to version with release_date {latest_version['release_date']}."
    )


def get_from_s3(s3_key: str) -> io.BytesIO:
    """Downloads a file from the asf-mission-data-tool S3 bucket and returns it as a BytesIO object.

    Parameters
    ----------
    s3_file_path : str
        The S3 key (file path) within the bucket.

    Returns
    -------
    io.BytesIO
        A file-like object containing the file content.
    """
    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket="asf-mission-data-tool", Key=s3_key)
    content = io.BytesIO(obj["Body"].read())
    return content


"""
Bronze
"""


def _save_provenance_to_toml(func: Callable[..., Any]) -> Callable[..., Any]:
    """
    A decorator function that adds provenance metadata to an S3 file upload operation.
    The following metadata about the downloaded file is captured:
    - Original file URL
    - Download date and time
    - Git user who downloaded the file
    This function generates a TOML file containing this metadata and uploads it to an S3 bucket.
    If a TOML file already exists in the bucket, the user is prompted to confirm whether to overwrite it.

    Parameters
    ----------
    func : Callable[..., Any]
        The decorated function that performs the primary operation, such as uploading a file to S3.

    Returns
    -------
    Callable[..., Any]
        The wrapper function that adds metadata handling to the original function.

    Raises
    -------
    Exception: If any error occurs during the file upload or metadata generation process.

    """

    def wrapper(*args: Any, **kwargs: Any) -> Any:

        # Execute inner function
        result = func(*args, **kwargs)

        # Extract information from arguments
        dataset_name = kwargs.get("dataset_name", "")
        target_url = kwargs.get("target_url", "")

        # Prepare provenance metadata
        metadata = {
            "original_file_url": target_url,
            "download_date_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "downloaded_by_git_user": subprocess.check_output(
                ["git", "config", "--global", "user.name"]
            )
            .strip()
            .decode("utf-8"),
        }

        # Convert to TOML format
        toml_content = toml.dumps(metadata)

        # Define S3 path for .toml file
        file_name = target_url.split("/")[-1]
        toml_file_name = file_name.rsplit(".", 1)[0] + ".toml"
        toml_file_path = f"bronze/{dataset_name}/{toml_file_name}"

        # Upload to S3
        s3_client = boto3.client("s3")

        # Check if .toml file already exists in S3 bucket
        try:
            s3_client.head_object(Bucket="asf-mission-data-tool", Key=toml_file_path)
            toml_exists = True
        except ClientError as error:
            if error.response["Error"]["Code"] == "404":
                toml_exists = False
            else:
                # Raise if other error
                raise

        # If .toml file exists, prompt user to confirm overwrite
        if toml_exists:
            overwrite = input(
                f"File s3://asf-mission-data-tool/{toml_file_path} already exists. Do you want to overwrite it? (y/n): "
            )
            if overwrite.lower() != "y":
                print("Metadata file not overwritten. Aborting upload.")
                return  # Abort the function

        # Proceed with uploading .toml file
        try:
            s3_client.put_object(
                Bucket="asf-mission-data-tool", Key=toml_file_path, Body=toml_content
            )
            print(
                f"Provenance metadata saved to s3://asf-mission-data-tool/{toml_file_path}"
            )
        except NoCredentialsError:
            print("Credentials not available.")
        except Exception as e:
            print(f"Error uploading metadata file: {e}")

        return result

    wrapper.__name__ = func.__name__
    wrapper.__doc__ = func.__doc__

    return wrapper


@_save_provenance_to_toml
def save_to_s3_bronze(dataset_name: str, target_url: str) -> str:
    """
    Downloads a file from a given URL and uploads it to the S3 bucket "asf-mission-data-tool".
    This function checks if the file already exists in the S3 bucket:
    - If the file exists and the size matches, it will ask the user if they want to overwrite it.
    - If the file exists but the size differs, it will ask the user if they still want to overwrite it.
    - If the file doesn't exist, it will upload the file.
    Additionally, it calls the `save_provenance_to_toml` decorator to save metadata about the file in a sidecar .toml file.
    Parameters
    ----------
    dataset_name : str
        The name of the dataset used for the S3 path; fixed, unique identifier to identify any instance of the dataset.
    target_url : str
        The URL of the file to be downloaded and uploaded to S3.

    Returns
    -------
    str
        File path where bronze data has been saved in s3.

    Raises
    -------
    Exception: If there are issues downloading the file, uploading to S3, or checking the file's existence.

    """

    # Download target file
    with requests.get(target_url) as response:
        if response.status_code == 200:
            file_content = response.content
        else:
            print(f"Failed to download file. Status code: {response.status_code}")
            return

    # Upload to S3
    s3_client = boto3.client("s3")

    # Get file name from URL or response headers
    if "Content-Disposition" in response.headers:
        content_disposition = response.headers["Content-Disposition"]
        file_name = content_disposition.split("filename=")[-1].strip('"')
    else:
        file_name = target_url.split("/")[-1]

    # Define S3 path for main file
    main_file_path = f"bronze/{dataset_name}/{file_name}"

    # Check if file already exists in S3 bucket
    try:
        response = s3_client.head_object(
            Bucket="asf-mission-data-tool", Key=main_file_path
        )
        # If no ClientError is raised, the file exists
        file_exists = True
        existing_file_size = response["ContentLength"]
    except ClientError as error:
        # If the ClientError code is 404, the file does not exist
        if error.response["Error"]["Code"] == "404":
            file_exists = False
        else:
            # Raise if other error
            raise

    # If main file exists, check if sizes match and prompt user to confirm overwrite
    if file_exists:
        local_file_size = len(file_content)
        if existing_file_size == local_file_size:
            print(
                f"File s3://asf-mission-data-tool/{main_file_path} already exists and has the same size."
            )
            overwrite = input(f"Do you want to overwrite it? (y/n): ")
            if overwrite.lower() != "y":
                print("Main file not overwritten. Aborting upload.")
                return  # Abort the function
        else:
            print(
                f"File s3://asf-mission-data-tool/{main_file_path} exists, but sizes do not match."
            )
            overwrite = input(f"Do you still want to overwrite it? (y/n): ")
            if overwrite.lower() != "y":
                print("Main file not overwritten. Aborting upload.")
                return  # Abort the function

    # Upload main file to S3 if file does not exist or overwrite confirmed
    try:
        s3_client.put_object(
            Bucket="asf-mission-data-tool", Key=main_file_path, Body=file_content
        )
        print(
            f"File uploaded successfully to s3://asf-mission-data-tool/{main_file_path}"
        )
    except NoCredentialsError:
        print("Credentials not available.")
    except Exception as e:
        print(f"Error uploading file: {e}")

    return f"s3://asf-mission-data-tool/{main_file_path}"


"""
Silver
"""


def save_to_s3_silver(
    dataframe: pd.DataFrame,
    dataset_name: str,
    main_file_dict: Dict,
    file_url_index: int,
    subset_id: str,
) -> str:
    """Saves a given Pandas DataFrame to the asf_mission_data_tool S3 bucket in parquet format. The file is stored in both
     the LATEST/ directory and a date-specific archive directory. Any existing file in LATEST/ that is older than five minutes
     is deemed as outdated and then deleted.

    Parameters
    ----------
    dataframe : pd.DataFrame
    Dataframe to be saved.
    dataset_name : str
        Name of dataset; must be a key of dataset in config/base.yaml.
    main_file_dict : Dict
        Dictionary containing dataset metadata from config/base.yaml.
    file_url_index : int
        Index of the target url in the file_url list in main_file_dict; to be used for the s3 file name.
    subset_id : str
        An identifier for the subset of the dataset.

    Returns
    -------
    str
        The S3 URI of the saved parquet file
    """

    # Extract file information
    file_name = (
        main_file_dict.get("file_url")[file_url_index].split("/")[-1].rsplit(".", 1)[0]
    )
    archive_date = pd.to_datetime(main_file_dict.get("release_date")).strftime("%B_%Y")

    s3_client = boto3.client("s3")
    bucket_name = "asf-mission-data-tool"

    # Save to LATEST/ and archive date/
    latest_prefix = f"silver/{dataset_name}/LATEST/"
    latest_key = f"silver/{dataset_name}/LATEST/{file_name}_{subset_id}.parquet"
    archive_key = (
        f"silver/{dataset_name}/{archive_date}/{file_name}_{subset_id}.parquet"
    )

    try:
        # List all files in the LATEST directory
        existing_files = s3_client.list_objects_v2(
            Bucket=bucket_name, Prefix=latest_prefix
        )

        if "Contents" in existing_files:
            objects_to_delete = []
            file_paths = []
            five_minutes_ago = datetime.now(timezone.utc) - timedelta(minutes=5)

            for obj in existing_files["Contents"]:
                last_modified = obj["LastModified"]

                if (
                    last_modified < five_minutes_ago
                ):  # Only delete if file is older than five minutes
                    objects_to_delete.append({"Key": obj["Key"]})
                    file_paths.append(f"s3://{bucket_name}/{obj['Key']}")

            if objects_to_delete:
                s3_client.delete_objects(
                    Bucket=bucket_name, Delete={"Objects": objects_to_delete}
                )

                print(
                    f"Deleted {len(objects_to_delete)} outdated files from {latest_prefix}:"
                )
                for path in file_paths:
                    print(f"  - {path}")

        with io.BytesIO() as parquet_buffer:

            dataframe.to_parquet(parquet_buffer, engine="pyarrow", index=False)
            parquet_buffer.seek(0)

            s3_client.put_object(
                Bucket=bucket_name,
                Key=latest_key,
                Body=parquet_buffer.getvalue(),
            )
            print(f"File uploaded successfully to s3://{bucket_name}/{latest_key}")
            s3_client.put_object(
                Bucket=bucket_name,
                Key=archive_key,
                Body=parquet_buffer.getvalue(),
            )
            print(f"File uploaded successfully to s3://{bucket_name}/{archive_key}")
    except NoCredentialsError:
        print("Credentials not available.")

    except Exception as e:
        print(f"Error uploading file: {e}")

    return f"s3://{bucket_name}/{archive_key}"
