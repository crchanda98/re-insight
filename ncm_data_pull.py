import pandas as pd
import time
import os
from datetime import datetime as dt, timedelta
import yaml
import utils

start_time = time.time()
CONFIG_PATH = os.environ.get("WEATHER_CONFIG", "reinsight_config.yml")
with open(CONFIG_PATH, "r") as f:
    config = yaml.safe_load(f)
username = config["ncm_user"]
password = config["ncm_password"]
base_url=config["base_url"]
data_func = utils.APICon(base_url = base_url)

url = "https://pdscloud.ncmrwf.gov.in:8443/api/v1/REdownload"
temp_dir = os.path.join(config["temp_dir"], "ncm_data")

df_static = pd.DataFrame.from_records(data_func.fetch_static_data())

csv_path = config["ncm_csv_data"]
ncm_temp_data= config["ncm_temp_data"]

if not os.path.exists(temp_dir):
    os.makedirs(temp_dir)

if not os.path.exists(ncm_temp_data):
    os.makedirs(ncm_temp_data)

manifest = []
if os.path.exists(config["ncm_data_log"]):
    with open(config["ncm_data_log"], "r") as f:
        manifest = f.read()
    if len(manifest) > 0:
        manifest = manifest.split("\n")

lag_days = 10
time_now = dt.now()
date_end = time_now.date()
date_start = date_end - timedelta(days=lag_days)
dates_str = [
    date_start + timedelta(days=i) for i in range((date_end - date_start).days + 1)
]

dates_str = [x.strftime("%Y%m%d") for x in dates_str]
# dates_str.reverse()  # Process latest date first
cycle = ["00", "06", "12", "18"]
# cycle.reverse()  # Process latest cycle first

for idate in dates_str:
    print(f"Processing date: {idate}")
    for icycle in cycle:
        date_name = idate + icycle
        if date_name in manifest:
            print(f"Data for date {date_name} already processed. Skipping...")
            continue
        else:
            print(f"Downloading data for date {date_name}")
            filename = utils.download_ncm_data(idate, icycle, temp_dir)
            if filename == "":
                print(f"Failed to download data for date {date_name}. Skipping...")
                continue
            else:
                file_path = os.path.join(temp_dir, filename)
                df_ncm = utils.extract_ncm(
                    fname=file_path, dest=ncm_temp_data, df_stn=df_static
                )
                csv_file_path = os.path.join(csv_path, f"ncm_{date_name}.csv")
                df_ncm.to_csv(csv_file_path, index=False)
                data_func.upload_weather_data(df_ncm)
                manifest.append(date_name)
                with open(config["ncm_data_log"], "w") as f:
                    f.write("\n".join(manifest))
