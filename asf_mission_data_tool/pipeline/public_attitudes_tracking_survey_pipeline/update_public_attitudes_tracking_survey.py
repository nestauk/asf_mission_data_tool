from asf_mission_data_tool.getters.data_getters import (
    get_page_url,
    get_file_url,
    get_release_date,
    add_new_version,
)

dataset_name = "public_attitudes_tracking_survey"
page_link_text = "DESNZ Public Attitudes Tracker:"
file_type = ".xlsx"
file_text = "DESNZ_Public_Attitudes_Tracker_"

page_url = get_page_url(
    dataset_name=dataset_name, page_link_text=page_link_text, page_link_index=1
)
file_url = get_file_url(page_url=page_url, file_type=file_type, file_text=file_text)
release_date = get_release_date(page_url)

seasons = ["winter", "spring", "summer"]
for season in seasons:
    if season in page_url:
        filter = season

add_new_version(
    dataset_name=dataset_name,
    page_url=page_url,
    file_url=file_url,
    release_date=release_date,
    filter=None,
)
