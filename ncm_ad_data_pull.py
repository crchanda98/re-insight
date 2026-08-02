import traceback
import requests
import argparse
from requests.auth import HTTPBasicAuth
import time
import os
from tqdm import tqdm
from datetime import datetime as dt, timedelta
import yaml
import pandas as pd
import utils
from sqlalchemy import create_engine
from urllib.parse import quote as urlquote
from pathlib import Path

start_time = time.time()

CONFIG_PATH = os.environ.get("WEATHER_CONFIG", "reinsight_config.yml")
SCRIPT_NAME = os.path.basename(__file__)
with open(CONFIG_PATH, "r") as f:
    CONFIG = yaml.safe_load(f)
EXTRACT_FROM_EXIST_DATA = True

username = CONFIG["ncm_ad_user"]
password = CONFIG["ncm_ad_password"]

db_cred = CONFIG["db_cred"]
engine = create_engine(
f"postgresql://{db_cred['user_name']}:%s@{db_cred['user_ip']}:{db_cred['user_port']}/{db_cred['db_name']}"
    % urlquote(db_cred["user_passwd"])
)

db_columns = CONFIG["db_columns"]
weather_table_column = db_columns["weather_table"]["columns"]
weather_table_column_un = db_columns["weather_table"]["unique_constraint"]

db_con = utils.DBcon(con = engine, db_schema=db_columns)
db_con.logging({"script": SCRIPT_NAME, "log_type": "info", "message": f"NCM AD FTP ETL script started"})

db_con = utils.DBcon(con = engine, db_schema=db_columns)

df_static = db_con.get_static_data()
df_static = df_static[df_static["parent_id"] != 0]

url = "https://pdscloud.ncmrwf.gov.in:8443/api/v1/REdownload"

root_path = os.path.join(CONFIG["temp_dir"], "ncm_ad_data")

model_data = os.path.join(root_path, "model_data")
csv_path = os.path.join(root_path, "csv_data")
ncm_temp_data= os.path.join(root_path, "temp_data")

os.makedirs(model_data, exist_ok=True)
os.makedirs(csv_path, exist_ok=True)
os.makedirs(ncm_temp_data, exist_ok=True)

parser = argparse.ArgumentParser(description="Pull NCM data")
parser.add_argument("--lag_days", type=int, default=2, help="Number of lag days to process")
args = parser.parse_args()

lag_days = args.lag_days
time_now = dt.now()
date_end = time_now.date()
date_start = date_end - timedelta(days=lag_days)
dates_str = [
    date_start + timedelta(days=i) for i in range((date_end - date_start).days + 1)
]

dates_str = [x.strftime("%Y%m%d") for x in dates_str]
cycle = ["00", "06", "12", "18"]

files = [
        {'url': url, 'variable': "data_adani_ncumg"},
        {'url': url, 'variable': "data_adani_ncumr"}
     ]

subdir_name = None
MODEL_MANIFEST = os.path.join(root_path, "ncm_ad.csv")

if os.path.exists(MODEL_MANIFEST):
    df_manifest = pd.read_csv(MODEL_MANIFEST, index_col=0)
else:
    df_manifest = pd.DataFrame(columns=["ncum_g", "ncum_r"])

dates_str.reverse()
cycle.reverse()

for idate in dates_str:
    print(f"Processing date: {idate}")
    for icycle in cycle:
        date_name = idate + icycle
        print(date_name)
        for file in files:
            if 'data_adani_ncumg' in file['variable']:
                subdir_name = "data_adani_ncumg"
                model_name = "ncum_g"
            if 'data_adani_ncumr' in file['variable']:
                subdir_name = "data_adani_ncumr"
                model_name = "ncum_r"
            print(f"Processing data for {idate}, {icycle}, {model_name}")
            
            if model_name in df_manifest.columns:
                existing_prediction_time = df_manifest[model_name].dropna().index.tolist()
                existing_prediction_time = [x.strftime("%Y%m%d%H") if isinstance(x, dt) else str(x) for x in existing_prediction_time]
            else:
                existing_prediction_time = []
            try:
                if date_name not in existing_prediction_time:
                    with requests.Session() as session:
                        session.auth = HTTPBasicAuth(username, password)
                    headers = {
                            'inputdate': idate,
                            'cycle': icycle,
                            'datavariable': subdir_name,
                            'api-key': 'cnGkcO3uIVNZsFvcEik8JrNS5tgf9s3n',
                            }
                    response = session.post(file['url'], headers=headers, stream=True)
                    if 'Content-Disposition' in response.headers:
                        cd = response.headers['Content-Disposition']
                        if 'filename=' in cd:
                            filename = cd.split('filename=')[1].strip('"')
                        else:
                            filename = file['filename'] 
                    else:
                        filename = file['filename'] 

                    if response.status_code == 200:
                        fileDownloadPath = os.path.join(model_data, model_name, idate)
                        os.makedirs(fileDownloadPath, exist_ok=True)
                        zip_path = os.path.join(fileDownloadPath, filename)
                        remote_size = int(response.headers.get('content-length', 0))
                        if os.path.exists(zip_path):
                            local_size = os.path.getsize(zip_path)
                        if not os.path.exists(zip_path) or local_size != remote_size:
                            with open(zip_path, 'wb') as f, tqdm(desc=filename, total=remote_size, unit='B', unit_scale=True) as pbar:
                                for chunk in response.iter_content(chunk_size=65536):
                                    if chunk: 
                                        f.write(chunk)   
                                        pbar.update(len(chunk))
                            local_size = os.path.getsize(zip_path)
                            print(f"Files downloaded and extracted successfully to {fileDownloadPath}")
                            db_con.logging({"script": SCRIPT_NAME, "log_type": "info", "message": f"Data downloaded for {model_name}, {date_name}"})
                        
                        if os.path.exists(zip_path) and local_size == remote_size:
                            db_con.logging({"script": SCRIPT_NAME, "log_type": "info", "message": f"Data avaialable for {model_name}, {date_name}"})
                            df_out = utils.extract_ncm_ad(fname = zip_path, dest = ncm_temp_data, df_stn=df_static, zone = model_name)
                            df_out.to_csv(os.path.join(csv_path, f"{idate}{icycle}_{model_name}.csv"), index=False)
                            df_db = pd.DataFrame(columns = db_columns["weather_table"]["columns"])
                            df_out = pd.concat([df_db, df_out])
                            df_out = df_out.dropna(axis=0, how="all")
                            df_out = df_out[db_columns["weather_table"]["columns"]]
                            df_out = df_out.set_index(db_columns["weather_table"]["unique_constraint"])
                            db_con.push_weather_data(df_out)
                            db_con.logging({"script": SCRIPT_NAME, "log_type": "info", "message": f"Data extracted for {model_name}, {date_name}"})
                            df_manifest.loc[date_name, model_name] = 1
                            dest_path = Path(ncm_temp_data)
                            for nc_file in dest_path.glob("*.nc"):
                                nc_file.unlink()
                        else:
                            print(f"File issue for {model_name}, {date_name}")
                    else:
                        print(f"Failed to download file: {response.status_code} - {response.text}")
                else:
                    print(f"{model_name} already exists for {date_name}")
            except:
                err = traceback.format_exc()
                db_con.logging({"script": SCRIPT_NAME, "log_type": "error", "message": f"Data not available for {model_name}, {date_name}"})
                print(f"Data not available for {model_name}, {date_name}")
                print(err)

end_time = time.time()
df_manifest.to_csv(MODEL_MANIFEST)
elapsed_time = end_time - start_time
print(f"Execution time: {elapsed_time} seconds")
