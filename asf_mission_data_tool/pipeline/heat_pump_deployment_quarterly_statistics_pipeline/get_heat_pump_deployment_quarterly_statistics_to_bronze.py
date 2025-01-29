from asf_mission_data_tool.getters.data_getters import (
    save_to_s3_bronze,
    get_latest_version,
    append_file_bronze_to_latest_version,
)

dataset_name = "heat_pump_deployment_quarterly_statistics"

latest_version = get_latest_version(dataset_name)

s3_file_path = []
for file_url in latest_version.get("file_url"):
    file_path = save_to_s3_bronze(dataset_name=dataset_name, target_url=file_url)
    if file_path:
        s3_file_path.append(file_path)

if not s3_file_path:
    pass
else:
    append_file_bronze_to_latest_version(
        dataset_name=dataset_name, s3_file_path=s3_file_path
    )
