import pandas as pd
import yaml
import os
import traceback
import json
import re
import numpy as np
from datetime import datetime as dt, timedelta
from sklearn.ensemble import RandomForestRegressor
from urllib.parse import quote as urlquote
from sqlalchemy import create_engine
from ftplib import FTP
import utils

CONFIG_PATH = os.getenv("WEATHER_CONFIG", "reinsight_config.yml")
with open(CONFIG_PATH, "r") as f:
    config = yaml.safe_load(f)

SCRIPT_NAME = os.path.basename(__file__)

FTP_HOST = config["fct_ftp_cred"]["host"]
FTP_USER = config["fct_ftp_cred"]["user"]
FTP_PASS = config["fct_ftp_cred"]["password"]

db_cred = config["db_cred"]

engine = create_engine(
    f"postgresql://{db_cred['user_name']}:%s@{db_cred['user_ip']}:{db_cred['user_port']}/{db_cred['db_name']}"
    % urlquote(db_cred["user_passwd"])
)
db_columns = config["db_columns"]
fct_table_column = db_columns["forecast_table"]["columns"]
fct_table_column_un = db_columns["forecast_table"]["unique_constraint"]

db_con = utils.DBcon(con = engine, db_schema=db_columns)
db_con.logging({"script": SCRIPT_NAME, "log_type": "info", "message": f"DA wind script started"})

df_static = db_con.get_static_data()
df_static["plant_details"] = df_static["plant_details"].apply(lambda x: re.sub(r'(\w+):', r'"\1":', x))
df_static["plant_details"] = df_static["plant_details"].apply(lambda x: json.loads(x))

date_now_ist = utils.get_last_15_min_slot()
date_now_utc = date_now_ist - timedelta(hours = 5, minutes = 30)
fct_start_time = date_now_ist.replace(hour = 0, minute = 0, second = 0, microsecond = 0) + timedelta(days = 1)
fct_end_time = fct_start_time + timedelta(hours = 24)
fct_start_time_utc = fct_start_time - timedelta(hours = 5, minutes = 30)
fct_end_time_utc = fct_end_time - timedelta(hours = 5, minutes = 30)


pc = pd.read_excel("./data/Vaayu_Power_Curve.xlsx")

pc = pc[["WS", "Standard Power"]]
pc.columns = ["wind","power"]

pc = pc.dropna()

pc["wind"] = pd.to_numeric(pc["wind"], errors="coerce")
pc["power"] = pd.to_numeric(pc["power"], errors="coerce")

pc = pc.dropna()
pc = pc.sort_values("wind")

pc_ws = pc["wind"].values
pc_pw = pc["power"].values   # kW → MW

# ----------------------------------------------------------
# WIND → POWER CONVERTER
# ----------------------------------------------------------

def ws_to_power(ws):
    ws_array = np.asarray(ws, dtype=float)
    return np.interp(ws_array, pc_ws, pc_pw, left=0, right=pc_pw[-1])

for _, idf in df_static.iterrows():
    try:
        print(f"Running DA for {idf['plant_name']}")
        db_con.logging({"script": SCRIPT_NAME, "log_type": "info", "message": f"Running DA for {idf['plant_name']}"})
        nwp_data = db_con.get_weather_data(plant=idf["plant_name"], model="ncm_d2", \
            start_date=fct_start_time_utc.strftime("%Y-%m-%dT%H:%M:%S"), \
            end_date=fct_end_time_utc.strftime("%Y-%m-%dT%H:%M:%S"))
        df_nwp = nwp_data[nwp_data["height"] == 80]
        df_nwp = df_nwp.sort_values(by=["forecast_time", "prediction_time"], ascending=[True, False])
        df_nwp = df_nwp.drop_duplicates(subset="forecast_time", keep="first")
        df_nwp["power"] = ws_to_power(df_nwp["wind_speed"]) * idf['plant_details']["turbine"]
        df_nwp = df_nwp[["forecast_time", "power"]]
        df_nwp["forecast_time"] = df_nwp["forecast_time"].dt.tz_convert("Asia/Kolkata")
        df_nwp["forecast_time"] = df_nwp["forecast_time"].dt.tz_localize(None)
        df_nwp = df_nwp.iloc[0:-1]
        fct_filename = f"../data_lake/re_insights/rel_time_fct/dayahead_wind_{idf['plant_name']}_{fct_start_time.strftime('%Y%m%d')}.csv"
        df_nwp.to_csv(fct_filename, index=False)
        db_con.logging({"script": SCRIPT_NAME, "log_type": "info", "message": f"DA data for {idf['plant_name']} generated"})
        utils.push_fct_to_ftp(fct_filename, FTP_HOST, FTP_USER, FTP_PASS)
        db_con.logging({"script": SCRIPT_NAME, "log_type": "success", "message": f"DA data for {idf['plant_name']} pushed to FTP"})
    except Exception as e:
        e = traceback.format_exc()
        print(f"An error occurred: {e}")
        db_con.logging({"script": SCRIPT_NAME, "log_type": "error", "message": f"DA data push failed for {idf['plant_name']}: {e}"})

db_con.logging({"script": SCRIPT_NAME, "log_type": "info", "message": f"DA wind script completed"})