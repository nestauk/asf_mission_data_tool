from asf_mission_data_tool.getters.data_getters import (
    get_page_url,
    get_file_url,
    get_release_date,
    add_new_version,
)

dataset_name = "heat_pump_deployment_quarterly_statistics"
page_link_text = "Heat pump deployment statistics:"
file_type = ".xlsx"
file_text = "Heat_pump_deployment_quarterly_statistics_United_Kingdom_"

page_url = get_page_url(dataset_name=dataset_name, page_link_text=page_link_text)
file_url = get_file_url(page_url=page_url, file_type=file_type, file_text=file_text)
release_date = get_release_date(page_url)

add_new_version(
    dataset_name=dataset_name,
    page_url=page_url,
    file_url=file_url,
    release_date=release_date,
    filter=None,
)
