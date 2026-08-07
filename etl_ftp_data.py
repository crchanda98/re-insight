import traceback
import pandas as pd
from datetime import datetime as dt, timedelta
import os
from ftplib import FTP
import utils
import argparse
import traceback
import yaml
from sqlalchemy import create_engine
from urllib.parse import quote as urlquote
import utils

SCRIPT_NAME = os.path.basename(__file__)

parser = argparse.ArgumentParser(description="Pull FTP data")
parser.add_argument("--lag_hours", type=int, default=2, help="Number of lag hours to process")
parser.add_argument("--lag_days", type=int, default=0, help="Number of lag days to process")

args = parser.parse_args()

lag_hours = args.lag_hours
lag_days = args.lag_days
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
db_con.logging({"script": SCRIPT_NAME, "log_type": "info", "message": f"Vayu FTP ETL script started"})

df_static = db_con.get_static_data()
df_static = df_static[df_static["parent_id"] != 0]

# --- Configuration ---
FTP_HOST = config["ftp_cred"]["host"]
FTP_USER = config["ftp_cred"]["user"]
FTP_PASS = config["ftp_cred"]["password"]

# Path based on your screenshot
LOCAL_DEST = f"../data_lake/re_insights/ftp_data"

end_time = utils.get_last_15_min_slot()

if lag_days == 0:
    start_time = end_time - timedelta(hours=lag_hours)
else:
    start_time = end_time - timedelta(days=lag_days)

REMOTE_DIR = f"/RealTime_SCADA/Loc_4094/"

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
    fname = os.path.basename(filename)
    plant_name  = fname[0:-18]
    record_time = dt.strptime(fname[-17:-4], "%Y%m%d_%H%M")
    df_all["record_time"] = [record_time]
    db_con.logging({"script": SCRIPT_NAME, "log_type": "info", "message": f"Latest measurement time for {plant_name}: {record_time}"})
    df_all["record_time"] = df_all["record_time"].dt.tz_localize("Asia/Kolkata")
    df_all["wind_speed"] = [idf["avgWind"].mean()]
    df_all["active_power"] = [idf["avgPower"].sum()]
    df_all["plant_name"] = [plant_name]
    df_all = pd.merge(df_all, df_static[["plant_name", "plant_id"]], on="plant_name")
    df_all = pd.concat([df_db, df_all])
    df_all = df_all.dropna(axis=0, how="all")
    df_all = df_all[columns]
    df_all = df_all.set_index(db_columns["meas_table"]["unique_constraint"])
    db_con.push_meas_data(df_all)
    db_con.logging({"script": SCRIPT_NAME, "log_type": "success", "message": f"Measurement data pushed for {plant_name}"})


def upload_meas_data_beempao(filename, plant_name):
    df_all = pd.read_csv(filename)
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
        
    df_all = df_all.rename({"Timestamp": "record_time", "Meter-PQ_Meter-100.Active_Power-kW": "active_power", "WMS-GTI-101.GTI-": "ghi"}, axis = 1)
    df_all["record_time"] = pd.to_datetime(df_all["record_time"])
    df_all["record_time"] = df_all["record_time"].dt.tz_localize("Asia/Kolkata")
    record_time_max = df_all["record_time"].max()
    db_con.logging({"script": SCRIPT_NAME, "log_type": "info", "message": f"Latest measurement time for {plant_name}: {record_time_max}"})
    df_all["active_power"] = df_all["active_power"] *1000
    df_all["plant_name"] = plant_name
    df_all = pd.merge(df_all, df_static[["plant_name", "plant_id"]], on="plant_name")
    df_all = pd.concat([df_db, df_all])
    df_all = df_all.dropna(axis=0, how="all")
    df_all = df_all[columns]
    df_all = df_all.set_index(db_columns["meas_table"]["unique_constraint"])
    db_con.push_meas_data(df_all)
    db_con.logging({"script": SCRIPT_NAME, "log_type": "success", "message": f"Measurement data pushed for {plant_name}"})

def upload_meas_data_beempao_minute_log(filename, plant_name):
    df_all = pd.read_csv(filename, index_col=False)
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
    df_all = df_all.rename({"DateTime": "record_time", "ACTIVE POWER": "active_power", "GHI": "ghi"}, axis = 1)
    df_all["record_time"] = pd.to_datetime(df_all["record_time"], format='%d-%m-%Y %H:%M')
    df_all["record_time"] = df_all["record_time"].dt.tz_localize("Asia/Kolkata")
    df_all = df_all.resample('15min', on='record_time', label='right', closed='left').agg({
        'active_power': 'mean',
        'ghi': 'mean'
    }).reset_index()
    record_time_max = df_all["record_time"].max()
    db_con.logging({"script": SCRIPT_NAME, "log_type": "info", "message": f"Latest measurement time for {plant_name}: {record_time_max}"})
    df_all["active_power"] = df_all["active_power"] *1000
    df_all["plant_name"] = plant_name
    df_all = pd.merge(df_all, df_static[["plant_name", "plant_id"]], on="plant_name")
    df_all = pd.concat([df_db, df_all])
    df_all = df_all.dropna(axis=0, how="all")
    df_all = df_all[columns]
    cols_to_numeric = ['active_power', 'ghi']
    df_all[cols_to_numeric] = df_all[cols_to_numeric].apply(pd.to_numeric, errors='coerce')
    df_all = df_all.round(2)
    df_all = df_all.set_index(db_columns["meas_table"]["unique_constraint"])
    db_con.push_meas_data(df_all)
    db_con.logging({"script": SCRIPT_NAME, "log_type": "success", "message": f"Measurement data pushed for {plant_name}"})


def download_ftp_directory(local_path, project_name, start, end):
    time_series = pd.date_range(start, end, freq="15min")
    # Create local directory if it doesn't exist
    if not os.path.exists(local_path):
        os.makedirs(local_path)

    try:
        with FTP(FTP_HOST) as ftp:
            ftp.login(user=FTP_USER, passwd=FTP_PASS)
            print(f"Connected to {FTP_HOST}")
            db_con.logging({"script": SCRIPT_NAME, "log_type": "info", "message": f"FTP server connected {project_name}"})
            for itime in time_series:
                for PLANT_NAME in ["Loc_4094", "Loc_4110", "Loc_4111"]:
                    try:
                        filename = itime.strftime("%Y%m%d_%H%M.csv")
                        local_dir = os.path.join(local_path, project_name)
                        local_filename = os.path.join(local_dir, PLANT_NAME + "_" + filename)
                        if not os.path.exists(local_dir):
                            os.makedirs(local_dir)
                        if os.path.exists(local_filename):
                            print(f"Exist {filename}...")
                            # upload_meas_data(local_filename)
                            continue
                        remote_path_date = (
                            f"/RealTime_SCADA/{PLANT_NAME}/{itime.strftime('%Y/%m/%d')}"
                        )
                        ftp.cwd(remote_path_date)
                        file_list_ftp = ftp.nlst()
                        print(f"Processing {filename}...")
                        if filename in file_list_ftp:
                            print(f"Found {filename} for {PLANT_NAME} on FTP server. Downloading")
                            with open(local_filename, "wb") as f:
                                ftp.retrbinary(f"RETR {filename}", f.write)
                            upload_meas_data(local_filename)
                        else:
                            print(f"Not Found {filename} for {PLANT_NAME} on FTP server.")
                    except Exception as e:
                        e = traceback.format_exc()
                        print(f"An error occurred: {e}")
                        print(local_filename)
                        continue

            print("\nDownload complete!")

    except Exception as e:
        e = traceback.format_exc()
        print(f"An error occurred: {e}")

def pull_sekura_scada_data(ftp_cred, project_name, local_path, start, end):
    FTP_HOST = ftp_cred["host"]
    FTP_USER = ftp_cred["user"]
    FTP_PASS = ftp_cred["password"]
    time_series = pd.date_range(start, end, freq="D")
    for itime in time_series:
        try:
            with FTP(FTP_HOST) as ftp:
                ftp.login(user=FTP_USER, passwd=FTP_PASS)
                print(f"Connected to {FTP_HOST}")
                db_con.logging({"script": SCRIPT_NAME, "log_type": "info", "message": f"FTP server connected for {project_name}"})
                for PLANT_NAME in ["Sekura_Agar350"]:
                    try:
                        filename = PLANT_NAME + "_" + itime.strftime("%Y%m%d.csv")
                        local_dir = os.path.join(local_path, project_name, PLANT_NAME)
                        if not os.path.exists(local_dir):
                            os.makedirs(local_dir)
                        local_filename = os.path.join(local_dir, filename)

                        if os.path.exists(local_filename):
                            print(f"Exist {filename}...")
                            # continue
                        remote_path_date = (
                            f"/FTP/{project_name}"
                        )
                        ftp.cwd(remote_path_date)
                        file_list_ftp = ftp.nlst()
                        print(f"Processing {filename}...")
                        if filename in file_list_ftp:
                            print(f"Found {filename} on FTP server.")
                            print(f"Downloading {filename}...")
                            with open(local_filename, "wb") as f:
                                ftp.retrbinary(f"RETR {filename}", f.write)
                            upload_meas_data_beempao(local_filename, PLANT_NAME)
                        else:
                            print(f"Not Found {filename} on FTP server.")
                    except Exception as e:
                        e = traceback.format_exc()
                        print(f"An error occurred: {e}")
        except Exception as e:
            e = traceback.format_exc()
            print(f"An error occurred: {e}")

def pull_sekura_scada_data_minute_log(ftp_cred, project_name, local_path, start, end, skip_local=False):
    FTP_HOST = ftp_cred["host"]
    FTP_USER = ftp_cred["user"]
    FTP_PASS = ftp_cred["password"]
    #### ADDING ONE DAY EXTRA AS FTP DATA IS UPLOADED ON DAILY BASIS
    #### WE ALSO DON'T CHECK LOCAL FILE AS
    start = start - timedelta(days=1)
    time_series = pd.date_range(start, end, freq="D").to_list()
    time_series.reverse()
    for itime in time_series:
        try:
            with FTP(FTP_HOST) as ftp:
                ftp.login(user=FTP_USER, passwd=FTP_PASS)
                print(f"Connected to {FTP_HOST}")
                db_con.logging({"script": SCRIPT_NAME, "log_type": "info", "message": f"FTP server connected for {project_name}"})
                for PLANT_NAME in ["Sekura_Agar350"]:
                    try:
                        filename = itime.strftime("%d-%m-%Y.csv")
                        local_dir = os.path.join(local_path, project_name, PLANT_NAME)
                        if not os.path.exists(local_dir):
                            os.makedirs(local_dir)
                        local_filename = os.path.join(local_dir, filename)
                        
                        if os.path.exists(local_filename):
                            print(f"Exist {filename}...")
                            if skip_local:
                                print("Skipping...")
                                continue

                        remote_path_date = (
                            f"/FTP/{project_name}"
                        )
                        ftp.cwd(remote_path_date)
                        file_list_ftp = ftp.nlst()
                        print(f"Processing {filename}...")
                        if filename in file_list_ftp:
                            print(f"Found {filename} on FTP server.")
                            print(f"Downloading {filename}...")
                            with open(local_filename, "wb") as f:
                                ftp.retrbinary(f"RETR {filename}", f.write)
                            upload_meas_data_beempao_minute_log(local_filename, PLANT_NAME)
                        else:
                            print(f"Not Found {filename} on FTP server.")
                    except Exception as e:
                        e = traceback.format_exc()
                        print(f"An error occurred: {e}")
        except Exception as e:
            e = traceback.format_exc()
            print(f"An error occurred: {e}")


if __name__ == "__main__":
    download_ftp_directory(LOCAL_DEST, project_name = "Vayu", start=start_time, end=end_time)
    pull_sekura_scada_data_minute_log(ftp_cred=config["sekura_scada_ftp_cred"], project_name="Agar", local_path=LOCAL_DEST, start=start_time, end=end_time)
    db_con.logging({"script": SCRIPT_NAME, "log_type": "info", "message": f"Vayu FTP ETL script completed"})