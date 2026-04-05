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

# --- Configuration ---
FTP_HOST = "115.113.175.188"
FTP_USER = "scada-fore"
FTP_PASS = "Forcast#0925SCADA"
PLANT_NAME = "Loc_4094"

# Path based on your screenshot
LOCAL_DEST = f"../data_lake/re_insights/ftp_data/{PLANT_NAME}"

parser = argparse.ArgumentParser(description="Pull NCM data")
parser.add_argument("--lag_hours", type=int, default=2, help="Number of lag hours to process")
args = parser.parse_args()

lag_hours = args.lag_hours
time_now = dt.now()
end_time = utils.get_last_15_min_slot()
start_time = end_time - timedelta(hours=lag_hours)
REMOTE_DIR = f"/RealTime_SCADA/Loc_4094/"

base_url = "http://127.0.0.1:5000"
data_func = utils.APICon(base_url=base_url)
df_static = pd.DataFrame.from_records(data_func.fetch_static_data())

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
    fname = filename.split("/")[-1][0:-4]
    df_all["record_time"] = [dt.strptime(fname, "%Y%m%d_%H%M")]
    df_all["wind_speed"] = [idf["avgWind"].mean()]
    df_all["active_power"] = [idf["avgPower"].sum()]
    df_all["plant_name"] = ["vayu"]
    df_all = pd.merge(df_all, df_static[["plant_name", "id"]], on="plant_name")
    df_all = df_all.rename({"id": "plant_id"}, axis=1)
    df_all = pd.concat([df_db, df_all])
    df_all = df_all.dropna(axis=0, how="all")
    df_all = df_all[columns]

    """Upload measurement data to meas_table via the API, converting timestamps to ISO strings."""
    # Prepare a copy with serializable timestamps
    df_serializable = df_all.copy()
    # Clean data: replace NaN/Inf with None for JSON compliance
    df_serializable = df_serializable.replace(
        {np.nan: None, np.inf: None, -np.inf: None}
    )
    if "record_time" in df_serializable.columns:
        df_serializable["record_time"] = df_serializable["record_time"].apply(
            lambda x: x.isoformat() if hasattr(x, "isoformat") else str(x)
        )
    payload = {"data": df_serializable.to_dict(orient="records")}
    url = "http://127.0.0.1:5000/meas/push"
    response = requests.post(url, json=payload)
    print(f"Status Code: {response.status_code}")
    try:
        print("Response:", response.json())
    except Exception:
        print("Response Text:", response.text)


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
                try:
                    filename = itime.strftime("%Y%m%d_%H%M.csv")
                    local_filename = os.path.join(local_path, filename)
                    if os.path.exists(local_filename):
                        print(f"Exist {filename}...")
                        upload_meas_data(local_filename)
                        continue
                    remote_path_date = (
                        f"/RealTime_SCADA/{PLANT_NAME}/{itime.strftime('%Y/%m/%d')}"
                    )
                    ftp.cwd(remote_path_date)
                    file_list_ftp = ftp.nlst()
                    print(f"Processing {filename}...")
                    if filename in file_list_ftp:
                        print(f"Found {filename} on FTP server.")
                        print(f"Downloading {filename}...")
                        with open(local_filename, "wb") as f:
                            ftp.retrbinary(f"RETR {filename}", f.write)
                        print(local_filename)
                        upload_meas_data(local_filename)
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
