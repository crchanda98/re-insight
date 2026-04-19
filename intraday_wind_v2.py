import pandas as pd
import yaml
import os
import traceback
import numpy as np
import utils
from datetime import datetime as dt, timedelta
from sklearn.ensemble import RandomForestRegressor
from urllib.parse import quote as urlquote
from sqlalchemy import create_engine
from ftplib import FTP

CONFIG_PATH = os.getenv("WEATHER_CONFIG", "reinsight_config.yml")
with open(CONFIG_PATH, "r") as f:
    config = yaml.safe_load(f)

FTP_HOST = config["fct_ftp_cred"]["host"]
FTP_USER = config["fct_ftp_cred"]["user"]
FTP_PASS = config["fct_ftp_cred"]["password"]

def push_fct_to_ftp(filename):
    with FTP(FTP_HOST) as ftp:
        ftp.login(user=FTP_USER, passwd=FTP_PASS)
        ftp.cwd("/home/ftpuser/ftp/upload")
        with open(filename, "rb") as file:
            ftp.storbinary(f"STOR {filename}", file)

db_cred = config["db_cred"]

engine = create_engine(
    f"postgresql://{db_cred['user_name']}:%s@{db_cred['user_ip']}:{db_cred['user_port']}/{db_cred['db_name']}"
    % urlquote(db_cred["user_passwd"])
)
db_columns = config["db_columns"]

db_con = utils.DBcon(con = engine, db_schema=db_columns)
df_static = db_con.get_static_data()

date_now_ist = utils.get_last_15_min_slot()
date_now = date_now_ist - timedelta(hours = 5, minutes = 30)
# date_now = date_now.replace(day = 27, month = 3)
fct_start_time = date_now + timedelta(hours = 2)
fct_end_time = fct_start_time + timedelta(hours = 6)
train_end_time = date_now
train_start_time = train_end_time - timedelta(days = 90)

base_url="http://127.0.0.1:5000"
farm_name = "Loc_4110"

data_func = utils.APICon(base_url = base_url)

### Fetching training data
meas_data = db_con.get_meas_data(plant=farm_name, \
    start_date=train_start_time.strftime("%Y-%m-%dT%H:%M:%S"), \
    end_date=train_end_time.strftime("%Y-%m-%dT%H:%M:%S"))
# df_meas = pd.DataFrame.from_records(meas_data)

### Fetching nwp model training data
nwp_data = db_con.get_weather_data(plant=farm_name, model="ncm_d2", \
    start_date=train_start_time.strftime("%Y-%m-%dT%H:%M:%S"), \
    end_date=train_end_time.strftime("%Y-%m-%dT%H:%M:%S"))
df_nwp = nwp_data[nwp_data["height"] == 80]
# assert False
# df_nwp = pd.DataFrame.from_records(nwp_data)
df_nwp = df_nwp.drop_duplicates(subset="forecast_time")
df_nwp['u_ncm_d2'] = df_nwp['wind_speed'] * np.cos(np.radians(270 - df_nwp['wind_direction']))
df_nwp['v_ncm_d2'] = df_nwp['wind_speed'] * np.sin (np.radians(270 - df_nwp['wind_direction']))
df_nwp['wind_speed_lag1'] = df_nwp["wind_speed"].shift(1)
df_nwp['wind_speed_lag4'] = df_nwp["wind_speed"].shift(4)
df_nwp['wind_speed_lag8'] = df_nwp["wind_speed"].shift(8)
df_nwp['hour'] = df_nwp["forecast_time"].dt.hour
df_nwp['doy'] = df_nwp["forecast_time"].dt.dayofyear


df_nwp = df_nwp[["forecast_time", "wind_speed", "wind_speed_lag1", "wind_speed_lag4", "wind_speed_lag8", "u_ncm_d2", "v_ncm_d2", "hour", "doy"]]
df_nwp = df_nwp.sort_values(by="forecast_time")
df_nwp = df_nwp.reset_index(drop=True)
df_nwp = df_nwp.rename({"forecast_time": "record_time", "wind_speed": "wind_speed_ncm_d2"}, axis = 1)


### Fetching nwp model forecast data
df_nwp_fct = db_con.get_weather_data(plant=farm_name, model="ncm_d2", \
    start_date=fct_start_time.strftime("%Y-%m-%dT%H:%M:%S"), \
    end_date=fct_end_time.strftime("%Y-%m-%dT%H:%M:%S"))

# df_nwp_fct = pd.DataFrame.from_records(df_nwp_fct)
df_nwp_fct = df_nwp_fct.drop_duplicates(subset="forecast_time")
df_nwp_fct['u_ncm_d2'] = df_nwp_fct['wind_speed'] * np.cos(np.radians(270 - df_nwp_fct['wind_direction']))
df_nwp_fct['v_ncm_d2'] = df_nwp_fct['wind_speed'] * np.sin (np.radians(270 - df_nwp_fct['wind_direction']))
df_nwp_fct['wind_speed_lag1'] = df_nwp_fct["wind_speed"].shift(1)
df_nwp_fct['wind_speed_lag4'] = df_nwp_fct["wind_speed"].shift(4)
df_nwp_fct['wind_speed_lag8'] = df_nwp_fct["wind_speed"].shift(8)
df_nwp_fct['hour'] = df_nwp_fct["forecast_time"].dt.hour
df_nwp_fct['doy'] = df_nwp_fct["forecast_time"].dt.dayofyear

df_nwp_fct = df_nwp_fct[["forecast_time", "wind_speed", "wind_speed_lag1", "wind_speed_lag4", "wind_speed_lag8", "u_ncm_d2", "v_ncm_d2", "hour", "doy"]]
df_nwp_fct = df_nwp_fct.drop_duplicates(subset="forecast_time")
df_nwp_fct = df_nwp_fct.sort_values(by="forecast_time")
df_nwp_fct = df_nwp_fct.reset_index(drop=True)
df_nwp_fct = df_nwp_fct.rename({"forecast_time": "record_time", "wind_speed": "wind_speed_ncm_d2"}, axis = 1)


df_train = pd.merge(meas_data, df_nwp, on="record_time", how="left")
features = ["u_ncm_d2", "v_ncm_d2", "wind_speed_lag1", "wind_speed_lag4", "wind_speed_lag8", "hour", "doy"]
features = ["wind_speed_lag1", "wind_speed_lag4", "wind_speed_lag8", "hour", "doy"]
target = "active_power"

X_train, y_train = df_train[features], df_train[target]

model = RandomForestRegressor(n_estimators=100, random_state=42)
model.fit(X_train, y_train)

X_fct = df_nwp_fct[features]
y_fct = model.predict(X_fct)
df_nwp_fct["predicted_power"] = y_fct
df_nwp_fct["plant_name"] = farm_name
df_nwp_fct = df_nwp_fct[["record_time", "predicted_power", "plant_name"]]
df_nwp_fct = df_nwp_fct.round(2)
df_nwp_fct["record_time"] = df_nwp_fct["record_time"].dt.tz_convert("Asia/Kolkata")
df_nwp_fct["record_time"] = df_nwp_fct["record_time"].dt.tz_localize(None)
df_nwp_fct.to_csv(f"../data_lake/re_insights/rel_time_fct/intraday_wind_{farm_name}_{date_now_ist.strftime('%Y%m%d_%H%M')}.csv", index = False)