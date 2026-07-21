import traceback
import pandas as pd
from datetime import datetime as dt, timedelta, date
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

SCRIPT_NAME = os.path.basename(__file__)

parser = argparse.ArgumentParser(description="Pull FTP data")
parser.add_argument("--lag_days", type=int, default=2, help="Number of lag days to process")

args = parser.parse_args()

def process_schedule_data(df):   
    df["plant_name"] = "Vayu"
    df["date"] = idate.strftime("%Y-%m-%d")
    df["forecast_time"] = df["TIME"].apply(lambda x: idate.strftime("%Y-%m-%d ") + x[0:5])
    df["prediction_time"] = idate.strftime("%Y-%m-%d 22:40")
    df["prediction_time"] = pd.to_datetime(df["prediction_time"]).dt.tz_localize("Asia/Kolkata")
    df["forecast_time"] = pd.to_datetime(df["forecast_time"]).dt.tz_localize("Asia/Kolkata")
    df["active_power"] = df["Revised Schedule (MW)"]
    df["forecast_source"] = "qca"
    df["model_name"] = "schedule"
    df["wind_speed"] = float("nan")
    df["ghi"] = float("nan")
    df["precipitation"] = float("nan")
    return df
    
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
plant_id = df_static[df_static["plant_name"] == "Vayu"]["plant_id"].iloc[0]

# --- Configuration ---
FTP_HOST = config["ftp_cred"]["host"]
FTP_USER = config["ftp_cred"]["user"]
FTP_PASS = config["ftp_cred"]["password"]

# Path based on your screenshot
LOCAL_DEST = f"../data_lake/re_insights/ftp_data"

date_end = date.today()
date_start = date_end -  timedelta(days=lag_days)

dates_str = [
    date_start + timedelta(days=i) for i in range((date_end - date_start).days + 1)
]

REMOTE_DIR = f"/Forecast/"
LOCAL_DIR = "../data_lake/re_insights/final_schedule/Vayu"

fct_table_column = db_columns["forecast_table"]["columns"]
fct_table_column_un = db_columns["forecast_table"]["unique_constraint"]


with FTP(FTP_HOST) as ftp:
    ftp.login(user=FTP_USER, passwd=FTP_PASS)
    print(f"Connected to ftp")
    for idate in dates_str:
        print(f"Processing schedule for {idate}")
        try:
            filename = idate.strftime("vayu_final_schedule_%Y%m%d.csv")
            local_filename = os.path.join(LOCAL_DIR, filename)
            remote_filename = idate.strftime("8001_%Y%m%d_R16.csv")
            if os.path.exists(local_filename):
                print(f"Schedule for {idate} already procesed")
                continue
            else:
                remote_dir = idate.strftime("/Forecast/%Y/%m/%d")
                ftp.cwd(remote_dir)
                ftp_filelist = ftp.nlst()
                if remote_filename in ftp_filelist:
                    with open(local_filename, "wb") as f:
                        ftp.retrbinary(f"RETR {remote_filename}", f.write)
                    df_power = pd.read_csv(local_filename)
                    df_power["plant_id"] = plant_id
                    df_power = process_schedule_data(df_power)
                    df_power = df_power[fct_table_column]
                    df_power = df_power.set_index(fct_table_column_un)
                    db_con.push_fct_data(df_power)
                    print(f"Processed schedule for {idate}")
                else:
                    print(f"Schedule for {idate} is not available in ftp")
        except:
            error = traceback.format_exc()
            print(f"Issue processing schedule for {idate}, error: {error}")