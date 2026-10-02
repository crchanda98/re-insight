import traceback
import requests
import argparse
from requests.auth import HTTPBasicAuth
import time
import os
from tqdm import tqdm
from datetime import datetime as dt, timedelta, timezone
import yaml
import pandas as pd
import utils
from sqlalchemy import create_engine
from urllib.parse import quote as urlquote
from pathlib import Path

start_time = time.time()

CONFIG_PATH = os.environ.get("WEATHER_CONFIG", "reinsight_config.yml")
SCRIPT_NAME = os.path.basename(__file__)

with open(CONFIG_PATH, "r") as f:
    CONFIG = yaml.safe_load(f)

EXTRACT_FROM_EXIST_DATA = True
GCS_FLAG = CONFIG["push_gcs"]

root_path = os.path.join(CONFIG["temp_dir"], "ncm_sat_data")

model_data = os.path.join(root_path, "model_data")
csv_path = os.path.join(root_path, "csv_data")
ncm_temp_data = os.path.join(root_path, "temp_data")

for path in [root_path, model_data, csv_path, ncm_temp_data]:
    os.makedirs(path, exist_ok=True)

if GCS_FLAG:
    gcs_utils = utils.GCSManager(bucket_name="re-insight-dev")

GCS_PATH = "satellite/ncm_sat"

username = CONFIG["ncm_sat_user"]
password = CONFIG["ncm_sat_password"]

db_cred = CONFIG["db_cred"]
engine = create_engine(
    f"postgresql://{db_cred['user_name']}:%s@{db_cred['user_ip']}:{db_cred['user_port']}/{db_cred['db_name']}"
    % urlquote(db_cred["user_passwd"])
)

db_columns = CONFIG["db_columns"]
weather_table_column = db_columns["weather_table"]["columns"]
weather_table_column_un = db_columns["weather_table"]["unique_constraint"]

db_con = utils.DBcon(con=engine, db_schema=db_columns)
db_con.logging(
    {
        "script": SCRIPT_NAME,
        "log_type": "info",
        "message": f"NCM AD FTP ETL script started",
    }
)

db_con = utils.DBcon(con=engine, db_schema=db_columns)

df_static = db_con.get_static_data()
df_static = df_static[df_static["parent_id"] != 0]


parser = argparse.ArgumentParser(description="Pull NCM data")
parser.add_argument(
    "--lag_hours", type=int, default=2, help="Number of lag days to process"
)
args = parser.parse_args()

lag_hours = args.lag_hours
time_now = dt.now(timezone.utc)
time_now = time_now - timedelta(
    minutes=time_now.minute % 15,
)
date_end = time_now
date_start = date_end - timedelta(hours=lag_hours)
dates_str = pd.date_range(start=date_start, end=date_end, freq="15min")

MODEL_MANIFEST = os.path.join(root_path, "ncm_sat.csv")

if os.path.exists(MODEL_MANIFEST):
    df_manifest = pd.read_csv(MODEL_MANIFEST, index_col=0)
else:
    df_manifest = pd.DataFrame(columns=["ncm_sat"])


# assert False
username = CONFIG["ncm_sat_user"]
password = CONFIG["ncm_sat_password"]

url = "https://pdscloud.ncmrwf.gov.in:8443/api/v1/REdownload"

files = [
    {"url": url, "variable": "satimage_data"},
]

for idate in dates_str:
    try:
        inputdate = idate.strftime("%Y%m%d")
        cycle = idate.strftime("%H%M")
        idate_str = idate.strftime("%Y%m%d%H%M")
        subdir_name = None

        existing_prediction_time = df_manifest["ncm_sat"].dropna().index.tolist()
        existing_prediction_time = [
            x.strftime("%Y%m%d%H") if isinstance(x, dt) else str(x)
            for x in existing_prediction_time
        ]

        if idate_str in existing_prediction_time and EXTRACT_FROM_EXIST_DATA:
            print(
                f"Data for {idate_str} already exists in manifest. Skipping download."
            )
            continue
        with requests.Session() as session:
            session.auth = HTTPBasicAuth(username, password)
            for file in files:
                if "satimage_data" in file["variable"]:
                    subdir_name = "satimage_data"
                headers = {
                    "inputdate": inputdate,
                    "cycle": cycle,
                    "datavariable": subdir_name,
                    "api-key": "QTxY3lk7AW1mZxFmKq4OxKPUJsPLla1",
                }
                response = session.post(file["url"], headers=headers, stream=True)
                if "Content-Disposition" in response.headers:
                    cd = response.headers["Content-Disposition"]
                    if "filename=" in cd:
                        filename = cd.split("filename=")[1].strip('"')
                    else:
                        filename = file["filename"]
                else:
                    filename = file["filename"]

                print("Filename from response:", filename)
                if response.status_code == 200:
                    fileDownloadPath = os.path.join(model_data, "ncm_sat", idate_str)
                    os.makedirs(fileDownloadPath, exist_ok=True)
                    nc_path = os.path.join(fileDownloadPath, filename)
                    gcs_path = os.path.join(GCS_PATH, "ncm_sat", idate_str, filename)
                    print("Filename is:", filename)
                    total_size = int(response.headers.get("content-length", 0))
                    with open(nc_path, "wb") as f, tqdm(
                        desc=filename, total=total_size, unit="B", unit_scale=True
                    ) as pbar:
                        for chunk in response.iter_content(chunk_size=65536):
                            if chunk:
                                f.write(chunk)
                                pbar.update(len(chunk))
                    print(
                        f"Files downloaded and extracted successfully to {fileDownloadPath}"
                    )
                    if GCS_FLAG:
                        gcs_utils.upload(nc_path, gcs_path)

                    df_out = utils.process_ncm_sat(
                        fname=nc_path,
                        df_stn=df_static,
                    )
                    df_out.to_csv(
                        os.path.join(csv_path, f"{idate}_ncm_sat.csv"),
                        index=False,
                    )
                    df_db = pd.DataFrame(columns=db_columns["weather_table"]["columns"])
                    df_out = df_out.set_index(
                        db_columns["weather_table"]["unique_constraint"]
                    )
                    db_con.push_weather_data(df_out)
                    db_con.logging(
                        {
                            "script": SCRIPT_NAME,
                            "log_type": "info",
                            "message": f"Data extracted for ncm_sat, {idate_str} and pushed to database",
                        }
                    )
                    df_manifest.loc[idate_str, "ncm_sat"] = 1

                else:
                    print(
                        f"Failed to download file: {response.status_code} - {response.text}"
                    )
    except Exception as e:
        print(f"Error occurred for date {idate_str}: {e}")
        traceback.print_exc()

end_time = time.time()
df_manifest.to_csv(MODEL_MANIFEST)
elapsed_time = end_time - start_time
print(f"Execution time: {elapsed_time} seconds")
