from asf_mission_data_tool.getters.data_getters import (
    save_to_s3_bronze,
    get_latest_version,
    append_file_bronze_to_latest_version,
)

dataset_name = "public_attitudes_tracking_survey"

# PAT is a triannual survey, get version for latest Summer/Spring/Winter

for season in ["Summer", "Spring", "Winter"]:
    latest_season_version = get_latest_version(dataset_name=dataset_name, filter=season)

    s3_file_path = []
    for url in latest_season_version.get("file_url"):
        file_path = save_to_s3_bronze(dataset_name=dataset_name, target_url=url)
        if file_path:
            s3_file_path.append(file_path)

        if not s3_file_path:
            pass
        else:
            append_file_bronze_to_latest_version(
                dataset_name=dataset_name, filter=season, s3_file_path=s3_file_path
            )
