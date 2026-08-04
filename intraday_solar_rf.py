import pandas as pd
import yaml
import os
import traceback
import numpy as np
import utils
from datetime import datetime as dt, timedelta
from urllib.parse import quote as urlquote
from sqlalchemy import create_engine
from ftplib import FTP
from sklearn.ensemble import RandomForestRegressor
import warnings
warnings.filterwarnings("ignore")

CONFIG_PATH = os.getenv("WEATHER_CONFIG", "reinsight_config.yml")
with open(CONFIG_PATH, "r") as f:
    config = yaml.safe_load(f)

SCRIPT_NAME = os.path.basename(__file__)

FTP_HOST = config["fct_ftp_cred"]["host"]
FTP_USER = config["fct_ftp_cred"]["user"]
FTP_PASS = config["fct_ftp_cred"]["password"]
MODEL_NAME = "intraday_solar_rf"

date_now = utils.get_last_15_min_slot()

def push_fct_to_ftp(filename):
    with FTP(FTP_HOST) as ftp:
        ftp.login(user=FTP_USER, passwd=FTP_PASS)
        ftp.cwd("/home/ftpuser/ftp/upload")
        with open(filename, "rb") as file:
            ftp.storbinary(f"STOR {filename}", file)

def proc_nwp_ghi(df_d, height):
    df_n = df_d[df_d["height"] == height]
    df_n = df_n.sort_values(by=["forecast_time", "prediction_time"], ascending=[True, False])
    df_n = df_n.drop_duplicates(subset="forecast_time", keep="first")
    df_n = df_n[["forecast_time", "ghi", "low_cloud", "medium_cloud"]]
    df_n = df_n.dropna(axis = 1, how = "all")
    df_n = df_n.set_index("forecast_time")
    df_n = df_n.resample("15min").interpolate(method="time")
    return df_n

def get_clear_sky(idf):
    idf["day_hour"] = idf.index.hour*100 + idf.index.minute
    idf_cs = idf.groupby("day_hour")[["active_power"]].max()
    idf_cs = idf_cs.reset_index()
    idf_cs = idf_cs.rename({"active_power": "active_power_cs"}, axis = 1)
    idf_cs = idf_cs[["day_hour", "active_power_cs"]]
    return idf_cs

db_cred = config["db_cred"]

engine = create_engine(
    f"postgresql://{db_cred['user_name']}:%s@{db_cred['user_ip']}:{db_cred['user_port']}/{db_cred['db_name']}"
    % urlquote(db_cred["user_passwd"])
)
db_columns = config["db_columns"]
fct_table_column = db_columns["forecast_table"]["columns"]
fct_table_column_un = db_columns["forecast_table"]["unique_constraint"]

db_con = utils.DBcon(con = engine, db_schema=db_columns)
db_con.logging({"script": SCRIPT_NAME, "log_type": "info", "message": f"Intraday wind script started"})

df_static = db_con.get_static_data()
df_static = df_static[df_static["plant_id"].isin([20])]

fct_start_time = date_now + timedelta(hours = 1, minutes = 30)
fct_end_time = fct_start_time + timedelta(hours = 6)

train_end_time = date_now
train_start_time = train_end_time - timedelta(days = 7)

fct_dir = f"../data_lake/re_insights/model_fct/{MODEL_NAME}"
os.makedirs(fct_dir, exist_ok=True)
EXOGEN_VAR = ["ghi"]

for _, idf in df_static.iterrows():
    try:
        print(f"Running ID for {idf['plant_name']}")
        db_con.logging({"script": SCRIPT_NAME, "log_type": "info", "message": f"Running ID for {idf['plant_name']}"})
        plant_name = idf["plant_name"]
        avc = idf["avc"] *1000
        df_static_plant = df_static[df_static["plant_name"] == plant_name].iloc[0]

        ### Fetching training data
        meas_data = db_con.get_meas_data(plant=plant_name, \
            start_date=train_start_time.strftime("%Y-%m-%dT%H:%M:%S"), \
            end_date=train_end_time.strftime("%Y-%m-%dT%H:%M:%S"))
        latest_ftp_time = meas_data["record_time"].max()
        meas_data = meas_data[["record_time", "active_power"]]
        meas_data.loc[meas_data["active_power"] < 0, "active_power"] = 0
        meas_data.loc[meas_data["active_power"] > avc, "active_power"] = float('nan')

        db_con.logging({"script": SCRIPT_NAME, "log_type": "info", "message": f"Meas data for {plant_name} with latest record time {latest_ftp_time}"})

        ### Fetching nwp model training data
        for i, imodel in enumerate(["ecmwf_ifs", "ecmwf_ifs025", "gfs_global", "icon_global", "ukmo_global_deterministic_10km", "meteofrance_arpege_world"]):
            nwp_data = db_con.get_weather_data(plant=plant_name, model=imodel, \
                    start_date=train_start_time.strftime("%Y-%m-%dT%H:%M:%S"), \
                    end_date=fct_end_time.strftime("%Y-%m-%dT%H:%M:%S"))
            df_nwp_i = proc_nwp_ghi(nwp_data, height=10)
            df_nwp_i = df_nwp_i.add_suffix("_" + imodel)
            df_nwp_i = df_nwp_i.reset_index()
            if i == 0:
                df_nwp = df_nwp_i
            else:
                df_nwp = pd.merge(df_nwp, df_nwp_i, on="forecast_time", how="left")
                
        nwp_data = db_con.get_weather_data(plant=plant_name, model="ncum_r", \
            start_date=train_start_time.strftime("%Y-%m-%dT%H:%M:%S"), \
            end_date=fct_end_time.strftime("%Y-%m-%dT%H:%M:%S"))
        df_nwp_i = proc_nwp_ghi(nwp_data, height=0)
        df_nwp_i = df_nwp_i.add_suffix("_" + "ncum_r")
        df_nwp_i = df_nwp_i.reset_index()
        df_nwp = pd.merge(df_nwp, df_nwp_i, on="forecast_time", how="left")

        nwp_data = db_con.get_weather_data(plant=plant_name, model="ncum_g", \
            start_date=train_start_time.strftime("%Y-%m-%dT%H:%M:%S"), \
            end_date=fct_end_time.strftime("%Y-%m-%dT%H:%M:%S"))
        df_nwp_i = proc_nwp_ghi(nwp_data, height=0)
        df_nwp_i = df_nwp_i.add_suffix("_" + "ncum_g")
        df_nwp_i = df_nwp_i.reset_index()
        df_nwp = pd.merge(df_nwp, df_nwp_i, on="forecast_time", how="left")
        df_nwp = df_nwp.rename({"forecast_time": "record_time"}, axis = 1)
        EXOGEN_VAR = [col for col in df_nwp.columns if "ghi" in col]
        
        df_model = pd.merge(meas_data, df_nwp, on="record_time", how="outer")
        df_model["record_time"] = df_model["record_time"].dt.tz_convert("Asia/Kolkata")
        df_model["record_time"] = df_model["record_time"].dt.tz_localize(None)

        df_model = df_model.set_index("record_time")
        meas_data_cs = get_clear_sky(df_model[["active_power"]].dropna())

        df_model = df_model.asfreq('15min')
        df_model = df_model.resample("15min").interpolate(method = "linear")

        df_train = df_model.loc[train_start_time.strftime("%Y-%m-%dT%H:%M:%S"):train_end_time.strftime("%Y-%m-%dT%H:%M:%S")]
        df_fct = df_model.loc[fct_start_time.strftime("%Y-%m-%dT%H:%M:%S"):fct_end_time.strftime("%Y-%m-%dT%H:%M:%S")]

        df_fct = df_fct.dropna(subset=EXOGEN_VAR)
        df_train = df_train.dropna(subset="active_power")
        
        model = RandomForestRegressor()
        model.fit(df_train[EXOGEN_VAR], df_train["active_power"])
        pred = model.predict(df_fct[EXOGEN_VAR])
        df_fct["active_power"] = pred
        db_fct = df_fct.reset_index()
        
        df_fct["active_power"] = pred
        df_fct = df_fct.dropna(subset="active_power")
        df_fct["day_hour"] = df_fct.index.hour*100 + df_fct.index.minute
        df_fct = df_fct.reset_index()
        df_fct = pd.merge(df_fct, meas_data_cs[["day_hour", "active_power_cs"]], on = "day_hour", how = "left")
        
        df_fct["active_power"][df_fct["active_power_cs"] ==0] = 0
        df_fct["active_power"][df_fct["active_power"] < 0] = 0
        df_fct.loc[df_fct["active_power_cs"] <= 0, "active_power"] = 0
        df_fct.loc[df_fct["active_power"] >= df_fct["active_power_cs"], "active_power"] = df_fct["active_power_cs"]
        df_fct = df_fct.reset_index()
        df_fct = df_fct.dropna(subset=["active_power"])
        df_fct["plant_name"] = plant_name
        df_fct["plant_id"] = df_static_plant["plant_id"]
        df_fct = df_fct[["record_time", "active_power", "plant_name", "plant_id"]]
        df_fct = df_fct.rename({"record_time": "forecast_time"}, axis = 1)
        df_fct = df_fct.round(2)
        df_fct["active_power"] = df_fct["active_power"].clip(lower = 0, upper = avc * 0.85)
        df_fct["forecast_time"] = df_fct["forecast_time"].dt.tz_localize("Asia/Kolkata")
        df_fct["prediction_time"] = date_now
        df_fct["prediction_time"] = df_fct["prediction_time"].dt.tz_localize("Asia/Kolkata")
        df_fct["forecast_source"] = "inhouse"
        df_fct["model_name"] = MODEL_NAME
        df_fct.to_csv(f"{fct_dir}/intraday_wind_{plant_name}_{date_now.strftime('%Y%m%d_%H%M')}.csv", index = False)

        df_all = pd.DataFrame(columns=fct_table_column)
        df_fct = pd.concat([df_all, df_fct], ignore_index=True)
        df_fct = df_fct.dropna(how = "all")
        df_fct = df_fct[fct_table_column]
        df_fct = df_fct.set_index(fct_table_column_un)
        db_con.push_fct_data(df_fct)
        db_con.logging({"script": SCRIPT_NAME, "log_type": "success", "message": f"FCT data generated for {plant_name}"})
    except Exception as e:
        e = traceback.format_exc()
        print(f"An error occurred: {e}")
        db_con.logging({"script": SCRIPT_NAME, "log_type": "error", "message": f"FCT data generation failed for {plant_name}: {e}"})

db_con.logging({"script": SCRIPT_NAME, "log_type": "info", "message": f"Intraday wind script completed"})