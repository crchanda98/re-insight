from IPython.core import display_functions
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
from darts import TimeSeries
from darts.models import LightGBMModel

CONFIG_PATH = os.getenv("WEATHER_CONFIG", "reinsight_config.yml")
with open(CONFIG_PATH, "r") as f:
    config = yaml.safe_load(f)

SCRIPT_NAME = os.path.basename(__file__)

FTP_HOST = config["fct_ftp_cred"]["host"]
FTP_USER = config["fct_ftp_cred"]["user"]
FTP_PASS = config["fct_ftp_cred"]["password"]
MODEL_NAME = "intraday_wind_ts"
EXOGEN_VAR = ["u_ecm_d2", "v_ecm_d2", "u_ncm_d2", "v_ncm_d2"]

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
fct_table_column = db_columns["forecast_table"]["columns"]
fct_table_column_un = db_columns["forecast_table"]["unique_constraint"]

db_con = utils.DBcon(con = engine, db_schema=db_columns)
db_con.logging({"script": SCRIPT_NAME, "log_type": "info", "message": f"Intraday wind script started"})

df_static = db_con.get_static_data()
df_static = df_static[df_static["plant_id"].isin([1, 2, 3])]

date_now = utils.get_last_15_min_slot()
fct_start_time = date_now + timedelta(hours = 1, minutes = 30)
fct_end_time = fct_start_time + timedelta(hours = 6)

train_end_time = date_now
train_start_time = train_end_time - timedelta(days = 5)

fct_dir = f"../data_lake/re_insights/model_fct/{MODEL_NAME}"
os.makedirs(fct_dir, exist_ok=True)


for _, idf in df_static.iterrows():
    try:
        print(f"Running ID for {idf['plant_name']}")
        db_con.logging({"script": SCRIPT_NAME, "log_type": "info", "message": f"Running ID for {idf['plant_name']}"})
        farm_name = idf["plant_name"]
        avc = idf["avc"] *1000
        df_static_plant = df_static[df_static["plant_name"] == farm_name].iloc[0]

        ### Fetching training data
        meas_data = db_con.get_meas_data(plant=farm_name, \
            start_date=train_start_time.strftime("%Y-%m-%dT%H:%M:%S"), \
            end_date=train_end_time.strftime("%Y-%m-%dT%H:%M:%S"))
        meas_data = meas_data[["record_time", "active_power"]]
        latest_ftp_time = meas_data["record_time"].max()
        db_con.logging({"script": SCRIPT_NAME, "log_type": "info", "message": f"Meas data for {farm_name} with latest record time {latest_ftp_time}"})

        nwp_data = db_con.get_weather_data(plant=farm_name, model="ecmwf_ifs", \
            start_date=train_start_time.strftime("%Y-%m-%dT%H:%M:%S"), \
            end_date=fct_end_time.strftime("%Y-%m-%dT%H:%M:%S"))
        df_nwp_ecm = nwp_data[nwp_data["height"] == 80]
        latest_pred_time = nwp_data["prediction_time"].max()
        df_nwp_ecm = df_nwp_ecm.sort_values(by=["forecast_time", "prediction_time"], ascending=[True, False])
        df_nwp_ecm = df_nwp_ecm.drop_duplicates(subset="forecast_time", keep="first")
        df_nwp_ecm = df_nwp_ecm[["forecast_time", "wind_speed", "wind_direction"]]
        df_nwp_ecm['u_ecm_d2'] = df_nwp_ecm['wind_speed'] * np.cos(np.radians(270 - df_nwp_ecm['wind_direction']))
        df_nwp_ecm['v_ecm_d2'] = df_nwp_ecm['wind_speed'] * np.sin (np.radians(270 - df_nwp_ecm['wind_direction']))
        df_nwp_ecm = df_nwp_ecm.set_index("forecast_time")
        df_nwp_ecm = df_nwp_ecm.resample("15min").interpolate(method="time")
        df_nwp_ecm = df_nwp_ecm.reset_index()
        print(latest_pred_time)

        db_con.logging({"script": SCRIPT_NAME, "log_type": "info", "message": f"Latest prediction time for ECMWF: {latest_pred_time}"})

        df_nwp_ecm = df_nwp_ecm[["forecast_time", "u_ecm_d2", "v_ecm_d2"]]
        df_nwp_ecm = df_nwp_ecm.drop_duplicates(subset="forecast_time")
        df_nwp_ecm = df_nwp_ecm.sort_values(by="forecast_time")
        df_nwp_ecm = df_nwp_ecm.rename({"forecast_time": "record_time"}, axis = 1)
        df_nwp_ecm = df_nwp_ecm.reset_index(drop=True)

        ### Fetching nwp model forecast data
        nwp_data = db_con.get_weather_data(plant=farm_name, model="ncm_d2", \
            start_date=train_start_time.strftime("%Y-%m-%dT%H:%M:%S"), \
            end_date=fct_end_time.strftime("%Y-%m-%dT%H:%M:%S"))
        df_nwp_ncm = nwp_data[nwp_data["height"] == 80]
        latest_pred_time = nwp_data["prediction_time"].max()

        df_nwp_ncm = df_nwp_ncm.sort_values(by=["forecast_time", "prediction_time"], ascending=[True, False])
        df_nwp_ncm = df_nwp_ncm.drop_duplicates(subset="forecast_time", keep="first")
        df_nwp_ncm = df_nwp_ncm[["forecast_time", "wind_speed", "wind_direction"]]
        df_nwp_ncm['u_ncm_d2'] = df_nwp_ncm['wind_speed'] * np.cos(np.radians(270 - df_nwp_ncm['wind_direction']))
        df_nwp_ncm['v_ncm_d2'] = df_nwp_ncm['wind_speed'] * np.sin (np.radians(270 - df_nwp_ncm['wind_direction']))
        df_nwp_ncm = df_nwp_ncm.set_index("forecast_time")
        df_nwp_ncm = df_nwp_ncm.resample("15min").interpolate(method="time")
        df_nwp_ncm = df_nwp_ncm.reset_index()
        print(latest_pred_time)

        db_con.logging({"script": SCRIPT_NAME, "log_type": "info", "message": f"Latest prediction time for NCMRWF: {latest_pred_time}"})

        df_nwp_ncm = df_nwp_ncm[["forecast_time", "u_ncm_d2", "v_ncm_d2"]]
        df_nwp_ncm = df_nwp_ncm.drop_duplicates(subset="forecast_time")
        df_nwp_ncm = df_nwp_ncm.sort_values(by="forecast_time")
        df_nwp_ncm = df_nwp_ncm.reset_index(drop=True)
        df_nwp_ncm = df_nwp_ncm.rename({"forecast_time": "record_time"}, axis = 1)


        df_model = pd.merge(meas_data, df_nwp_ecm, on="record_time", how="outer")
        df_model = pd.merge(df_model, df_nwp_ncm, on="record_time", how="outer")
        
        df_model["record_time"] = df_model["record_time"].dt.tz_convert("Asia/Kolkata")
        df_model["record_time"] = df_model["record_time"].dt.tz_localize(None)

        df_model = df_model.set_index("record_time")

        df_model = df_model.asfreq('15min')
        df_model = df_model.resample("15min").interpolate(method = "linear")

        df_train = df_model.loc[train_start_time.strftime("%Y-%m-%dT%H:%M:%S"):train_end_time.strftime("%Y-%m-%dT%H:%M:%S")]
        df_fct = df_model.loc[train_start_time.strftime("%Y-%m-%dT%H:%M:%S"):fct_end_time.strftime("%Y-%m-%dT%H:%M:%S")]
        df_fct = df_fct.dropna(subset=EXOGEN_VAR)
        df_train = df_train.dropna(subset="active_power")
        
        target_series_deploy = TimeSeries.from_dataframe(df_train, value_cols="active_power")
        covariates_series_deploy = TimeSeries.from_dataframe(df_fct, value_cols=EXOGEN_VAR)
        # covariates_series_deploy.columns = [str(col) for col in covariates_series_deploy.columns]

        model = LightGBMModel(
            lags=[-2, -3, -4, -5],
            lags_future_covariates=[
                0,
                1,
                2,
                3,
                4,
            ],
            verbose=-1,
            verbosity=-1,
            random_state=42,
        )

        model.fit(series=target_series_deploy,
            future_covariates=covariates_series_deploy)
        
        pred = model.predict(n=12)

        df_fct["active_power"] = pred.to_series()
        df_fct = df_fct.reset_index()
        df_fct = df_fct.dropna(subset=["active_power"])
        df_fct["plant_name"] = farm_name
        df_fct["plant_id"] = df_static_plant["plant_id"]
        df_fct = df_fct[["record_time", "active_power", "plant_name", "plant_id"]]
        df_fct = df_fct.rename({"record_time": "forecast_time"}, axis = 1)
        df_fct = df_fct.round(2)
        df_fct["active_power"] = df_fct["active_power"].clip(lower = avc * 0.15, upper = avc * 0.85)
        df_fct["forecast_time"] = df_fct["forecast_time"].dt.tz_localize("Asia/Kolkata")
        df_fct["prediction_time"] = date_now
        df_fct["prediction_time"] = df_fct["prediction_time"].dt.tz_localize("Asia/Kolkata")
        df_fct["forecast_source"] = "inhouse"
        df_fct["model_name"] = MODEL_NAME
        db_con.logging({"script": SCRIPT_NAME, "log_type": "info", "message": f"FCT data for {farm_name} with latest prediction time {latest_pred_time}"})
        df_fct.to_csv(f"{fct_dir}/intraday_wind_{farm_name}_{date_now.strftime('%Y%m%d_%H%M')}.csv", index = False)

        df_all = pd.DataFrame(columns=fct_table_column)
        df_fct = pd.concat([df_all, df_fct], ignore_index=True)
        df_fct = df_fct.dropna(how = "all")
        df_fct = df_fct[fct_table_column]
        df_fct = df_fct.set_index(fct_table_column_un)
        db_con.logging({"script": SCRIPT_NAME, "log_type": "info", "message": f"FCT data for {farm_name} with latest prediction time {latest_pred_time}"})
        db_con.push_fct_data(df_fct)
        db_con.logging({"script": SCRIPT_NAME, "log_type": "success", "message": f"FCT data generated for {farm_name}"})
    except Exception as e:
        e = traceback.format_exc()
        print(f"An error occurred: {e}")
        db_con.logging({"script": SCRIPT_NAME, "log_type": "error", "message": f"FCT data generation failed for {farm_name}: {e}"})

db_con.logging({"script": SCRIPT_NAME, "log_type": "info", "message": f"Intraday wind script completed"})