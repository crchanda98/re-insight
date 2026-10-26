import pandas as pd
import yaml
import os
import traceback
import utils
from datetime import timedelta
from urllib.parse import quote as urlquote
from sqlalchemy import create_engine
from ftplib import FTP
import xgboost as xgb
from zoneinfo import ZoneInfo

CONFIG_PATH = os.getenv("WEATHER_CONFIG", "reinsight_config.yml")
LOGGING = True
with open(CONFIG_PATH, "r") as f:
    config = yaml.safe_load(f)

SCRIPT_NAME = os.path.basename(__file__)

FTP_HOST = config["fct_ftp_cred"]["host"]
FTP_USER = config["fct_ftp_cred"]["user"]
FTP_PASS = config["fct_ftp_cred"]["password"]
MODEL_NAME = "wind_weekahead"
NWP_MODELS = ["cmc_gem_gdps", "ecmwf_ifs", "icon_global", "gfs_global"]
target_folder_ftp = "/home/ftpuser/ftp/upload/Wind/Vayu/Week-Ahead"


def proc_nwp_ws(df_n, height):
    df_n = df_n[df_n["height"] == height]
    df_n["day_num"] = (df_n["forecast_time"] - df_n["prediction_time"]).dt.days
    df_n = df_n.sort_values(
        by=["forecast_time", "prediction_time"], ascending=[True, False]
    )
    df_n = df_n.drop_duplicates(subset=["forecast_time", "day_num"], keep="first")
    df_n = df_n[["forecast_time", "wind_speed", "model_name", "day_num"]]
    return df_n


def push_fct_to_ftp(filename, target_folder):
    base_name = os.path.basename(filename)
    try:
        with FTP(FTP_HOST) as ftp:
            ftp.login(user=FTP_USER, passwd=FTP_PASS)
            ftp.cwd(target_folder)
            with open(filename, "rb") as file:
                ftp.storbinary(f"STOR {base_name}", file)
    except Exception as e:
        e = traceback.format_exc()
        print(f"An error occurred: {e}")


db_cred = config["db_cred"]

engine = create_engine(
    f"postgresql://{db_cred['user_name']}:%s@{db_cred['user_ip']}:{db_cred['user_port']}/{db_cred['db_name']}"
    % urlquote(db_cred["user_passwd"])
)
db_columns = config["db_columns"]
fct_table_column = db_columns["forecast_table"]["columns"]
fct_table_column_un = db_columns["forecast_table"]["unique_constraint"]

db_con = utils.DBcon(con=engine, db_schema=db_columns)
db_con.logging(
    {
        "script": SCRIPT_NAME,
        "log_type": "info",
        "message": f"Intraday wind script started",
    }
)

df_static = db_con.get_static_data()
df_static = df_static[df_static["plant_id"].isin([1, 2, 3])]

date_now = utils.get_last_15_min_slot()
date_now = date_now.replace(tzinfo=ZoneInfo("Asia/Kolkata"))

fct_start_time = date_now + timedelta(days=1)
fct_start_time = fct_start_time.replace(hour=0, minute=0)
fct_end_time = fct_start_time + timedelta(days=7)

train_end_time = date_now
train_start_time = train_end_time - timedelta(days=30)

fct_dir = f"../data_lake/re_insights/model_fct/{MODEL_NAME}"
os.makedirs(fct_dir, exist_ok=True)

all_plants_forecast = []
for _, idf in df_static.iterrows():
    try:
        print(f"Running WA for {idf['plant_name']}")
        db_con.logging(
            {
                "script": SCRIPT_NAME,
                "log_type": "info",
                "message": f"Running ID for {idf['plant_name']}",
            }
        )
        plant_name = idf["plant_name"]
        avc = idf["avc"] * 1000
        df_static_plant = df_static[df_static["plant_name"] == plant_name].iloc[0]

        ### Fetching training data
        meas_data = db_con.get_meas_data(
            plant=plant_name,
            start_date=train_start_time.strftime("%Y-%m-%dT%H:%M:%S"),
            end_date=train_end_time.strftime("%Y-%m-%dT%H:%M:%S"),
        )
        meas_data = meas_data[["record_time", "active_power"]]
        latest_ftp_time = meas_data["record_time"].max()
        meas_data = meas_data.rename(columns={"record_time": "forecast_time"})
        if LOGGING:
            db_con.logging(
                {
                    "script": SCRIPT_NAME,
                    "log_type": "info",
                    "message": f"Meas data for {plant_name} with latest record time {latest_ftp_time}",
                }
            )

        df_nwp = []
        for nwp_model in NWP_MODELS:
            nwp_data = db_con.get_weather_data(
                plant=plant_name,
                model=nwp_model,
                start_date=train_start_time.strftime("%Y-%m-%dT%H:%M:%S"),
                end_date=fct_end_time.strftime("%Y-%m-%dT%H:%M:%S"),
            )
            nwp_data = proc_nwp_ws(nwp_data, height=80)
            df_nwp.append(nwp_data)
        df_nwp = pd.concat(df_nwp)
        df_nwp = df_nwp.pivot(
            index=["forecast_time", "day_num"],
            columns="model_name",
            values="wind_speed",
        )
        df_nwp = df_nwp.reset_index()
        df_forecast = []
        for iday in range(1, 10):
            df_nwp_iday = df_nwp[df_nwp["day_num"] == iday]
            df_nwp_iday = df_nwp_iday.drop("day_num", axis=1)
            df_nwp_iday = df_nwp_iday.set_index("forecast_time")
            df_nwp_iday = df_nwp_iday.resample("15min").interpolate(method="linear")
            df_nwp_iday = df_nwp_iday.reset_index()
            df_nwp_iday = df_nwp_iday.dropna(axis=1, how="all")
            features = [x for x in df_nwp_iday.columns if x != "forecast_time"]
            df_model = pd.merge(df_nwp_iday, meas_data, on="forecast_time", how="left")
            df_model["forecast_time"] = df_model["forecast_time"].dt.tz_convert(
                "Asia/Kolkata"
            )
            df_train = df_model.dropna()
            df_test = df_model[df_model["forecast_time"] >= fct_start_time]
            if len(df_test) == 0:
                print(iday, "no data")
                continue
            X = df_train[features]
            y = df_train["active_power"]

            model = xgb.XGBRegressor(
                n_estimators=150,
                learning_rate=0.05,
                max_depth=3,
                min_child_weight=5,
                subsample=0.8,
                colsample_bytree=0.8,
                reg_lambda=1.5,
                random_state=42,
                objective="reg:squarederror",
            )
            model.fit(X, y)
            forecast = model.predict(df_test[features])
            df_test["active_power"] = forecast
            df_forecast.append(df_test)
        df_forecast = pd.concat(df_forecast)
        df_forecast = df_forecast[df_forecast["forecast_time"] >= fct_start_time]
        df_forecast = df_forecast[df_forecast["forecast_time"] < fct_end_time]
        df_ftp = df_forecast[["forecast_time", "active_power"]]
        fct_filename = f"../data_lake/re_insights/rel_time_fct/weekahead_wind_rf_{plant_name}_{MODEL_NAME}_{fct_start_time.strftime('%Y%m%d')}.csv"
        df_ftp.to_csv(fct_filename, index=False)
        push_fct_to_ftp(fct_filename, target_folder_ftp)
        all_plants_forecast.append(df_forecast)
    except:
        error = traceback.format_exc()
        print(error)

all_plants_forecast = pd.concat(all_plants_forecast, ignore_index=True)
all_plants_forecast = all_plants_forecast.reset_index(drop=True)
all_plants_forecast["prediction_time"] = date_now
all_plants_forecast["forecast_source"] = "inhouse"
all_plants_forecast["model_name"] = MODEL_NAME
df_all = pd.DataFrame(columns=fct_table_column)
all_plants_forecast = pd.concat([df_all, all_plants_forecast], ignore_index=True)
all_plants_forecast = all_plants_forecast[fct_table_column]
all_plants_forecast = all_plants_forecast.drop_duplicates(
    subset=fct_table_column_un, keep="last"
)
all_plants_forecast = all_plants_forecast.set_index(fct_table_column_un)
db_con.push_fct_data(all_plants_forecast)

print(f"✅ Forecast successfully generated")
