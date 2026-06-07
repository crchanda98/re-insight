import pandas as pd
import yaml
import os
import traceback
import numpy as np
import utils
from datetime import datetime as dt, timedelta
from sqlalchemy import create_engine
from urllib.parse import quote as urlquote

CONFIG_PATH = os.getenv("WEATHER_CONFIG", "reinsight_config.yml")
with open(CONFIG_PATH, "r") as f:
    config = yaml.safe_load(f)

SCRIPT_NAME = os.path.basename(__file__)

date_now_ist = utils.get_last_15_min_slot()
date_now_utc = date_now_ist - timedelta(hours = 5, minutes = 30)


fct_end_time = date_now_utc + timedelta(hours = 6)
fct_start_time = date_now_utc - timedelta(hours = 6)

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

meas_table_column = db_columns["meas_table"]["columns"]
meas_table_column_un = db_columns["meas_table"]["unique_constraint"]

db_con = utils.DBcon(con = engine, db_schema=db_columns)

db_con.logging({"script": SCRIPT_NAME, "log_type": "info", "message": f"Intraday aggregator script started"})

df_static = db_con.get_static_data()
df_hybrid = df_static[df_static["parent_id"] == 0]

group_cols_model = [
    "prediction_time",
    "forecast_time",
    "forecast_source",
    "model_name",
]

group_cols_meas = [
    "record_time",
]

for _, idf_parent in df_hybrid.iterrows():
    try:
        df_static_child = df_static[df_static["parent_id"] == idf_parent['plant_id']]
        db_con.logging({"script": SCRIPT_NAME, "log_type": "info", "message": f"Running aggrigator for {idf_parent['plant_name']}"})
        
        df_hybrid = []
        print(f"Running model aggrigator for {idf_parent['plant_name']}")
        for _, idf_child in df_static_child.iterrows():
            farm_name = idf_child["plant_name"]
            for imodel in ["om_xgb", "intraday_wind"]:
                fct_data = db_con.get_fct_data(plant=farm_name, fct_src="inhouse", model_name=imodel, \
                    start_date=fct_start_time.strftime("%Y-%m-%dT%H:%M:%S"), end_date=fct_end_time.strftime("%Y-%m-%dT%H:%M:%S"))
                fct_data = fct_data.drop(['created_at', 'plant_name', 'plant_id'], axis = 1)
                df_hybrid.append(fct_data)
        df_hybrid = pd.concat(df_hybrid)
        df_hybrid = (
            df_hybrid.groupby(group_cols_model)
            .agg({"active_power": "sum", "wind_speed": "mean",  'ghi': "mean", 'precipitation': "mean"})
            .reset_index()
        )
        df_hybrid["plant_name"] = idf_parent["plant_name"]
        df_hybrid["plant_id"] = idf_parent["plant_id"]
        df_hybrid = df_hybrid[fct_table_column]
        df_hybrid = df_hybrid.set_index(fct_table_column_un)
        db_con.push_fct_data(df_hybrid)
        db_con.logging({"script": SCRIPT_NAME, "log_type": "success", "message": f"Pushed aggregated forecast data for {farm_name}"})


        #### AGGREGATE MEAS
        df_hybrid = []
        print(f"Running meas aggrigator for {idf_parent['plant_name']}")
        for _, idf_child in df_static_child.iterrows():
            farm_name = idf_child["plant_name"]
            df_meas = db_con.get_meas_data(plant=farm_name, start_date=fct_start_time.strftime("%Y-%m-%dT%H:%M:%S"), \
                end_date=fct_end_time.strftime("%Y-%m-%dT%H:%M:%S"))

            df_meas = df_meas.drop(['created_at', 'plant_name', 'plant_id'], axis = 1)
            df_hybrid.append(df_meas)
        df_hybrid = pd.concat(df_hybrid)
        df_hybrid = (
            df_hybrid.groupby(group_cols_meas)
            .agg({"active_power": "sum", "wind_speed": "mean",  
                "wind_direction": "mean",
                "ghi": "mean",
                "gii": "mean",
                "humidity": "mean",
                "temperature": "mean",
                'precipitation': "mean"})
            .reset_index()
        )
        df_hybrid["plant_name"] = idf_parent["plant_name"]
        df_hybrid["plant_id"] = idf_parent["plant_id"]
        df_hybrid = df_hybrid[meas_table_column]
        df_hybrid = df_hybrid.set_index(meas_table_column_un)
        db_con.push_meas_data(df_hybrid)
        db_con.logging({"script": SCRIPT_NAME, "log_type": "success", "message": f"Pushed aggregated meas data for {farm_name}"})
    except Exception as e:
        e = traceback.format_exc()
        db_con.logging({"script": SCRIPT_NAME, "log_type": "error", "message": f"Aggregator failed for {farm_name}: {e}"})
        print(e)