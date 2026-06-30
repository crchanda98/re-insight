import yaml
import os
import traceback
import utils
from datetime import timedelta
from ftplib import FTP
from sqlalchemy import create_engine
from urllib.parse import quote as urlquote

CONFIG_PATH = os.getenv("WEATHER_CONFIG", "reinsight_config.yml")
with open(CONFIG_PATH, "r") as f:
    config = yaml.safe_load(f)

SCRIPT_NAME = os.path.basename(__file__)

date_now = utils.get_last_15_min_slot()
fct_start_time = date_now + timedelta(hours = 1, minutes = 30)
fct_end_time = fct_start_time.replace(hour = 23, minute = 45)

FTP_HOST = config["fct_ftp_cred"]["host"]
FTP_USER = config["fct_ftp_cred"]["user"]
FTP_PASS = config["fct_ftp_cred"]["password"]

db_cred = config["db_cred"]

engine = create_engine(
    f"postgresql://{db_cred['user_name']}:%s@{db_cred['user_ip']}:{db_cred['user_port']}/{db_cred['db_name']}"
    % urlquote(db_cred["user_passwd"])
)
db_columns = config["db_columns"]

db_con = utils.DBcon(con = engine, db_schema=db_columns)
db_con.logging({"script": SCRIPT_NAME, "log_type": "info", "message": f"FCT dispatch script started"})

def push_fct_to_ftp(filename):
    base_name = os.path.basename(filename)
    try:
        with FTP(FTP_HOST) as ftp:
            ftp.login(user=FTP_USER, passwd=FTP_PASS)
            ftp.cwd("/home/ftpuser/ftp/upload")
            with open(filename, "rb") as file:
                ftp.storbinary(f"STOR {base_name}", file)
            db_con.logging({"script": SCRIPT_NAME, "log_type": "success", "message": f"FCT data pushed to FTP for {filename}"})
    except Exception as e:
        e = traceback.format_exc()
        print(f"An error occurred: {e}")
        db_con.logging({"script": SCRIPT_NAME, "log_type": "error", "message": f"FCT data push failed for {filename}: {e}"})

df_static = db_con.get_static_data()
df_static = df_static[df_static["parent_id"].isin([1, 2, 3])]

for _, idf in df_static.iterrows():
    try:
        print(f"Running ID for {idf['plant_name']}")
        farm_name = idf["plant_name"]
        fct_filename = f"../data_lake/re_insights/fct_dispatch/intraday_wind_{farm_name}_{date_now_ist.strftime('%Y%m%d_%H%M')}.csv"
        fct_data = db_con.get_fct_data(plant=farm_name, fct_src="inhouse", model_name="intraday_wind", \
            start_date=fct_start_time.strftime("%Y-%m-%dT%H:%M:%S"), \
        end_date=fct_end_time.strftime("%Y-%m-%dT%H:%M:%S"))
        fct_data = fct_data.sort_values(by=["forecast_time", "prediction_time"], ascending=[True, False])
        fct_data = fct_data.drop_duplicates(subset=["forecast_time"], keep = "first")
        latest_pred_time = fct_data['prediction_time'].max()
        db_con.logging({"script": SCRIPT_NAME, "log_type": "info", "message": f"Latest prediction time for {farm_name}: {latest_pred_time}"})
        fct_data["forecast_time"] = fct_data["forecast_time"].dt.tz_convert("Asia/Kolkata")
        fct_data = fct_data[["plant_name", "forecast_time", "active_power"]]
        fct_data.to_csv(fct_filename, index = False)
        db_con.logging({"script": SCRIPT_NAME, "log_type": "info", "message": f"{len(fct_data)} records found for {farm_name}"})
        push_fct_to_ftp(fct_filename)
    except Exception as e:
        e = traceback.format_exc()
        print(f"An error occurred: {e}")
        db_con.logging({"script": SCRIPT_NAME, "log_type": "error", "message": f"FCT data push failed for {farm_name}: {e}"})
    
db_con.logging({"script": SCRIPT_NAME, "log_type": "info", "message": f"FCT dispatch script completed"})