import requests
import boto3
import subprocess
import toml
import logging
import yaml
from botocore.exceptions import NoCredentialsError, ClientError
from datetime import datetime
from typing import Callable, Any, Optional, Dict
from asf_mission_data_tool import config

# Suppress information level messages from botocore
logging.getLogger("botocore.credentials").setLevel(logging.ERROR)


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

    if filter:
        filtered_versions = [
            version
            for version in versions
            if any(filter in url for url in version["file_url"])
        ]
        latest_version = max(
            filtered_versions,
            key=lambda x: datetime.strptime(x["release_date"], "%Y-%m-%d"),
        )
    else:
        latest_version = max(
            versions, key=lambda x: datetime.strptime(x["release_date"], "%Y-%m-%d")
        )
    return latest_version


def append_file_bronze_to_latest_version(
    dataset_name: str,
    s3_file_path: str,
    filter: Optional[str] = None,
) -> None:

    # Load existing config data
    with open("asf_mission_data_tool/config/base.yaml", "r") as file:
        all_existing_data = yaml.safe_load(file)

    # Update data
    dataset_specific_data = all_existing_data.get("dataset", {}).get(dataset_name, {})
    versions = dataset_specific_data.get("versions", [])
    if filter:
        filtered_versions = [
            version
            for version in versions
            if any(filter in url for url in version["file_url"])
        ]
        latest_version = max(
            filtered_versions,
            key=lambda x: datetime.strptime(x["release_date"], "%Y-%m-%d"),
        )
    else:
        latest_version = max(
            versions, key=lambda x: datetime.strptime(x["release_date"], "%Y-%m-%d")
        )
    latest_version["file_bronze"] = s3_file_path

    # Write updated data to .yaml
    with open("asf_mission_data_tool/config/base.yaml", "w") as file:
        yaml.dump(all_existing_data, file, default_flow_style=False, sort_keys=True)

    print(
        f"file_bronze field added to version with release_date {latest_version['release_date']}."
    )
