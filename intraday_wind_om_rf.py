import pandas as pd
import yaml
import os
import traceback
import utils
from datetime import datetime as dt, timedelta
from urllib.parse import quote as urlquote
from sqlalchemy import create_engine
from ftplib import FTP
import joblib
import argparse

parser = argparse.ArgumentParser(description="Pull NCM data")
parser.add_argument("--lag_hours", type=int, default=0, help="Number of lag days to process")
args = parser.parse_args()

CONFIG_PATH = os.getenv("WEATHER_CONFIG", "reinsight_config.yml")
LOGGING = False
with open(CONFIG_PATH, "r") as f:
    config = yaml.safe_load(f)

SCRIPT_NAME = os.path.basename(__file__)
MODEL_NAME = "om_xgb"
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
db_con.logging({"script": SCRIPT_NAME, "log_type": "info", "message": f"Intraday wind script started"})
df_static = db_con.get_static_data()
df_static = df_static[df_static["parent_id"] != 0]

lag_hours = args.lag_hours

date_now_ist = utils.get_last_15_min_slot(dt_now = dt.now() - timedelta(hours = lag_hours))
date_now = date_now_ist - timedelta(hours = 5, minutes = 30)
fct_start_time = date_now + timedelta(hours = 1, minutes = 30)
fct_time_values = pd.date_range(start=fct_start_time, freq='15min', periods=12)
fct_end_time = fct_start_time + timedelta(hours = 6)


train_end_time = date_now
train_start_time = train_end_time - timedelta(hours = 1)
print(f"Running model for IST: {date_now_ist}, UTC: {date_now}")
print(f"Forecast wndow: {fct_start_time} UTC, {fct_end_time} UTC")

features = ['wind_speed_120m_ecmwf_ifsms',
    'wind_speed_80m_ecmwf_ifsms',
    'wind_speed_120m_gfs_globalms',
    'wind_speed_80m_gfs_globalms',
    'wind_speed_120m_icon_globalms',
    'wind_speed_80m_icon_globalms',
    'wind_speed_120m_meteofrance_arpege_worldms',
    'wind_speed_80m_meteofrance_arpege_worldms',
    'wind_speed_120m_gem_globalms',
    'wind_speed_80m_gem_globalms',
    'power_lag_1',
    'power_lag_2',
    'power_lag_3']
fct_out_dir = f"../data_lake/re_insights/real_time_fct/{MODEL_NAME}"
os.makedirs(fct_out_dir, exist_ok=True)

for _, idf in df_static.iterrows():
    try:
        farm_name = idf["plant_name"]
        model_path = f"../data_lake/re_insights/models/vayuu/{MODEL_NAME}/{MODEL_NAME}_{farm_name}_20260531.joblib"
        print(f"Running ID for {idf['plant_name']}")
        if LOGGING:
            db_con.logging({"script": SCRIPT_NAME, "log_type": "info", "message": f"Running ID for {idf['plant_name']}"})

        df_static_plant = df_static[df_static["plant_name"] == farm_name].iloc[0]

        ### Fetching training data
        meas_data = db_con.get_meas_data(plant=farm_name, \
            start_date=train_start_time.strftime("%Y-%m-%dT%H:%M:%S"), \
            end_date=train_end_time.strftime("%Y-%m-%dT%H:%M:%S"))
        lag_time1, lag_time2 = meas_data["record_time"].iloc[-1], meas_data["record_time"].iloc[-2]
        lag_time1 = lag_time1.tz_convert(None)
        lag_time2 = lag_time2.tz_convert(None)
        data_lag = (date_now - lag_time1).total_seconds()/60
        if LOGGING:
            db_con.logging({"script": SCRIPT_NAME, "log_type": "info", "message": f"Measurement data for {farm_name} with latest meas data from {lag_time1}"})
            db_con.logging({"script": SCRIPT_NAME, "log_type": "info", "message": f"Data lag {data_lag} mins"})

        df_opnemeteo_json = utils.get_openmeteo(15.03533, 77.993)
        df_opnemeteo_json = df_opnemeteo_json["hourly"]
        df_opnemeteo = pd.DataFrame(df_opnemeteo_json)

        df_opnemeteo.columns = df_opnemeteo.columns.str.replace(r'[^a-zA-Z0-9_]', '', regex=True)
        df_opnemeteo["time"] = pd.to_datetime(df_opnemeteo["time"])
        df_opnemeteo = df_opnemeteo.set_index("time")
        df_opnemeteo = df_opnemeteo.resample('15min').interpolate('linear')
        df_opnemeteo.columns = [x + "ms" for x in df_opnemeteo.columns]
        filtered_cols = [col for col in df_opnemeteo.columns if 'wind_speed' in col]
        df_fct_X = df_opnemeteo[filtered_cols]
        df_fct_X = df_fct_X.loc[date_now:date_now]
        df_fct_X["power_lag_1"] = meas_data["active_power"].iloc[-1]
        df_fct_X["power_lag_2"] = meas_data["active_power"].iloc[-2]
        df_fct_X["power_lag_3"] = meas_data["active_power"].iloc[-3]
        df_fct_X = df_fct_X[features]
        loaded_model = joblib.load(model_path)
        power = loaded_model.predict(df_fct_X)
        power = power.T.flatten()
        
        df_power = pd.DataFrame(columns = ["active_power"], data=power)
        df_power["plant_name"] = farm_name
        df_power["plant_id"] = df_static_plant["plant_id"]
        
        df_power["forecast_time"] = fct_time_values
        df_power["forecast_time"] = df_power["forecast_time"].dt.tz_localize("UTC")
        df_power["prediction_time"] = date_now
        df_power["prediction_time"] = df_power["prediction_time"].dt.tz_localize("UTC")
        df_power["forecast_source"] = "inhouse"
        df_power["model_name"] = MODEL_NAME
        
        df_all = pd.DataFrame(columns=fct_table_column)
        df_power = pd.concat([df_all, df_power], ignore_index=True)
        df_power = df_power.dropna(how = "all")
        df_power = df_power[fct_table_column]
        df_power = df_power.set_index(fct_table_column_un)
        fct_out_path = os.path.join(fct_out_dir, f"intraday_wind_{farm_name}_{MODEL_NAME}_{date_now_ist.strftime('%Y%m%d_%H%M')}.csv")
        df_power.to_csv(fct_out_path, index = False)
        db_con.push_fct_data(df_power)
        
    except Exception as e:
        e = traceback.format_exc()
        print(f"An error occurred: {e}")
        db_con.logging({"script": SCRIPT_NAME, "log_type": "error", "message": f"FCT data generation failed for {farm_name}: {e}"})

db_con.logging({"script": SCRIPT_NAME, "log_type": "info", "message": f"Intraday wind script completed"})