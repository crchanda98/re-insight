import requests
import argparse
from requests.auth import HTTPBasicAuth
import time, zipfile
import os, sys
from tqdm import tqdm
from datetime import datetime as dt, timedelta
import yaml
import pandas as pd

start_time = time.time()

CONFIG_PATH = os.environ.get("WEATHER_CONFIG", "reinsight_config.yml")
SCRIPT_NAME = os.path.basename(__file__)
with open(CONFIG_PATH, "r") as f:
    CONFIG = yaml.safe_load(f)

username = CONFIG["ncm_ad_user"]
password = CONFIG["ncm_ad_password"]

url = "https://pdscloud.ncmrwf.gov.in:8443/api/v1/REdownload"
root_path = CONFIG["ad_ncumg_ncumr_output"]
temp_dir = os.path.join(CONFIG["temp_dir"], "ncm_ad_data")

csv_path = CONFIG["ncm_csv_data"]
ncm_temp_data= CONFIG["ncm_temp_data"]

if not os.path.exists(temp_dir):
    os.makedirs(temp_dir)

if not os.path.exists(ncm_temp_data):
    os.makedirs(ncm_temp_data)

manifest = []
if os.path.exists(CONFIG["adani_ncm_log"]):
    with open(CONFIG["adani_ncm_log"], "r") as f:
        manifest = f.read()
    if len(manifest) > 0:
        manifest = manifest.split("\n")

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
MODEL_MANIFEST = "../data_lake/re_insights/manifest_files/ncm_ad.csv"
if os.path.exists(MODEL_MANIFEST):
    df_manifest = pd.read_csv(MODEL_MANIFEST, index_col=0)
else:
    df_manifest = pd.DataFrame(columns=["ncum_g", "ncum_r"])

for idate in dates_str:
    print(f"Processing date: {idate}")
    for icycle in cycle:
        date_name = idate + icycle
        print(date_name)
        if date_name not in manifest:
            with requests.Session() as session:
                session.auth = HTTPBasicAuth(username, password)
                for file in files:
                    if 'data_adani_ncumg' in file['variable']:
                        subdir_name = "data_adani_ncumg"
                        model_name = "ncum_g"
                    if 'data_adani_ncumr' in file['variable']:
                        subdir_name = "data_adani_ncumr"
                        model_name = "ncum_r"
                    if model_name in df_manifest.columns:
                        existing_prediction_time = df_manifest[model_name].dropna().index.tolist()
                        existing_prediction_time = [x.strftime("%Y%m%d%H") if isinstance(x, dt) else str(x) for x in existing_prediction_time]
                    else:
                        existing_prediction_time = []
                    
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
                        fileDownloadPath = os.path.join(root_path, idate)
                        os.makedirs(fileDownloadPath, exist_ok=True)
                        zip_path = os.path.join(fileDownloadPath, filename)
                        total_size = int(response.headers.get('content-length', 0))
                        with open(zip_path, 'wb') as f, tqdm(desc=filename, total=total_size, unit='B', unit_scale=True) as pbar:
                            for chunk in response.iter_content(chunk_size=65536):
                                if chunk: 
                                    f.write(chunk)   
                                    pbar.update(len(chunk))   
                        print(f"Files downloaded and extracted successfully to {fileDownloadPath}")
                        df_manifest.loc[date_name, model_name] = 1
                    else:
                        print(f"Failed to download file: {response.status_code} - {response.text}")

end_time = time.time()
df_manifest.to_csv(MODEL_MANIFEST)
elapsed_time = end_time - start_time
print(f"Execution time: {elapsed_time} seconds")
