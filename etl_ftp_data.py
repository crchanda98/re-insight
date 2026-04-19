import traceback
import pandas as pd
from datetime import datetime as dt, timedelta
import os
from ftplib import FTP
import requests
import numpy as np
import utils
import argparse
import traceback
import yaml
from sqlalchemy import create_engine
from urllib.parse import quote as urlquote
import utils

CONFIG_PATH = os.environ.get("WEATHER_CONFIG", "reinsight_config.yml")

with open(CONFIG_PATH, "r") as f:
    config = yaml.safe_load(f)

db_cred = config["db_cred"]

engine = create_engine(
    f"postgresql://{db_cred['user_name']}:%s@{db_cred['user_ip']}:{db_cred['user_port']}/{db_cred['db_name']}"
    % urlquote(db_cred["user_passwd"])
)
db_columns = config["db_columns"]

db_con = utils.DBcon(con = engine, db_schema=db_columns)
df_static = db_con.get_static_data()

# --- Configuration ---
FTP_HOST = config["ftp_cred"]["host"]
FTP_USER = config["ftp_cred"]["user"]
FTP_PASS = config["ftp_cred"]["password"]

# Path based on your screenshot
LOCAL_DEST = f"../data_lake/re_insights/ftp_data"

parser = argparse.ArgumentParser(description="Pull FTP data")
parser.add_argument("--lag_hours", type=int, default=2, help="Number of lag hours to process")
args = parser.parse_args()

lag_hours = args.lag_hours
time_now = dt.now()
end_time = utils.get_last_15_min_slot()
start_time = end_time - timedelta(hours=lag_hours)
REMOTE_DIR = f"/RealTime_SCADA/Loc_4094/"

base_url = "http://127.0.0.1:5000"
data_func = utils.APICon(base_url=base_url)
# df_static = pd.DataFrame.from_records(data_func.fetch_static_data())

# config_file = "/Users/arijitchanda/Desktop/work/git_arijit/data_lake/re_insights/reinsight_config.yml"
# config = yaml.safe_load(open(config_file))
# db_cred = config["db_cred"]
# db_cred

def upload_meas_data(filename):
    idf = pd.read_csv(filename)
    columns=[
            "plant_id",
            "record_time",
            "active_power",
            "wind_speed",
            "wind_direction",
            "ghi",
            "humidity",
            "temperature",
            "precipitation",
        ]
    df_db = pd.DataFrame(columns = columns)
        
    df_all = pd.DataFrame()
    fname = filename.split("/")[-1][-17:-4]
    df_all["record_time"] = [dt.strptime(fname, "%Y%m%d_%H%M")]
    df_all["record_time"] = df_all["record_time"].dt.tz_localize("Asia/Kolkata")
    df_all["wind_speed"] = [idf["avgWind"].mean()]
    df_all["active_power"] = [idf["avgPower"].sum()]
    df_all["plant_name"] = [filename.split("/")[-1][0:-18]]
    df_all = pd.merge(df_all, df_static[["plant_name", "plant_id"]], on="plant_name")
    df_all = pd.concat([df_db, df_all])
    df_all = df_all.dropna(axis=0, how="all")
    df_all = df_all[columns]
    df_all = df_all.set_index(db_columns["meas_table"]["unique_constraint"])
    print(df_all)
    db_con.push_meas_data(df_all)

    # """Upload measurement data to meas_table via the API, converting timestamps to ISO strings."""
    # # Prepare a copy with serializable timestamps
    # df_serializable = df_all.copy()
    # # Clean data: replace NaN/Inf with None for JSON compliance
    # df_serializable = df_serializable.replace(
    #     {np.nan: None, np.inf: None, -np.inf: None}
    # )
    # if "record_time" in df_serializable.columns:
    #     df_serializable["record_time"] = df_serializable["record_time"].apply(
    #         lambda x: x.isoformat() if hasattr(x, "isoformat") else str(x)
    #     )
    # payload = {"data": df_serializable.to_dict(orient="records")}
    # url = "http://127.0.0.1:5000/meas/push"
    # response = requests.post(url, json=payload)
    # print(f"Status Code: {response.status_code}")
    # try:
    #     print("Response:", response.json())
    # except Exception:
    #     print("Response Text:", response.text)


def download_ftp_directory(local_path, start, end):
    time_series = pd.date_range(start, end, freq="15min")
    # Create local directory if it doesn't exist
    if not os.path.exists(local_path):
        os.makedirs(local_path)

    try:
        with FTP(FTP_HOST) as ftp:
            ftp.login(user=FTP_USER, passwd=FTP_PASS)
            print(f"Connected to {FTP_HOST}")

            for itime in time_series:
                for PLANT_NAME in ["Loc_4094", "Loc_4110", "Loc_4111"]:
                    try:
                        filename = itime.strftime("%Y%m%d_%H%M.csv")
                        local_filename = os.path.join(local_path, PLANT_NAME + "_" + filename)
                        if os.path.exists(local_filename):
                            print(f"Exist {filename}...")
                            upload_meas_data(local_filename)
                            continue
                        remote_path_date = (
                            f"/RealTime_SCADA/{PLANT_NAME}/{itime.strftime('%Y/%m/%d')}"
                        )
                        ftp.cwd(remote_path_date)
                        file_list_ftp = ftp.nlst()
                        # print(file_list_ftp)
                        print(f"Processing {filename}...")
                        if filename in file_list_ftp:
                            print(f"Found {filename} on FTP server.")
                            print(f"Downloading {filename}...")
                            with open(local_filename, "wb") as f:
                                ftp.retrbinary(f"RETR {filename}", f.write)
                            print(local_filename)
                            upload_meas_data(local_filename)
                        else:
                            print(f"Not Found {filename} on FTP server.")
                    except Exception as e:
                        e = traceback.format_exc()
                        print(f"An error occurred: {e}")
                        print(local_filename)
                        continue

            print("\nDownload complete!")

    except Exception as e:
        e = traceback.format_exc()
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    download_ftp_directory(LOCAL_DEST, start=start_time, end=end_time)
