import requests
from requests.auth import HTTPBasicAuth
import pandas as pd
import numpy as np
import xarray as xr
import tarfile
import os
from tqdm import tqdm
from datetime import datetime as dt, timedelta
import yaml
import traceback


def download_ncm_data(inputdate, cycle, data_path):
    filename = ""
    CONFIG_PATH = os.environ.get("WEATHER_CONFIG", "reinsight_config.yml")
    with open(CONFIG_PATH, "r") as f:
        config = yaml.safe_load(f)
    username = config["ncm_user"]
    password = config["ncm_password"]
    url = "https://pdscloud.ncmrwf.gov.in:8443/api/v1/REdownload"

    files = [
        {"url": url, "variable": "data_mal"},
    ]
    subdir_name = None
    try:
        with requests.Session() as session:
            session.auth = HTTPBasicAuth(username, password)
            for file in files:
                if "data_mal" in file["variable"]:
                    subdir_name = "data_mal"
                headers = {
                    "inputdate": inputdate,
                    "cycle": cycle,
                    "datavariable": subdir_name,
                    "api-key": "FxSBtvr9Yls2HqWaOLM9PzXCdWDPNMLs2",
                }
                response = session.post(file["url"], headers=headers, stream=True)
                print("Response:", response)
                if "Content-Disposition" in response.headers:
                    cd = response.headers["Content-Disposition"]
                    if "filename=" in cd:
                        filename = cd.split("filename=")[1].strip('"')
                    else:
                        filename = file["filename"]
                else:
                    filename = file["filename"]

                # print("Filename from response:", filename)
                if response.status_code == 200:
                    print("Filename is:", filename)
                    zip_path = os.path.join(data_path, filename)
                    total_size = int(response.headers.get("content-length", 0))
                    with open(zip_path, "wb") as f, tqdm(
                        desc=filename, total=total_size, unit="B", unit_scale=True
                    ) as pbar:
                        for chunk in response.iter_content(chunk_size=65536):
                            if chunk:
                                f.write(chunk)
                                pbar.update(len(chunk))
                    print(f"Files downloaded and extracted successfully to {data_path}")
                    return filename
                else:
                    print(
                        f"Failed to download file: {response.status_code} - {response.text}"
                    )
                    return filename
    except Exception as e:
        e = traceback.format_exc()
        print(e)
        return filename


def extract_ncm(fname, dest, df_stn):
    all_columns = [
        "plant_id",
        "prediction_time",
        "forecast_time",
        "height",
        "model_name",
        "wind_speed",
        "wind_direction",
        "ghi",
        "humidity",
        "temperature",
        "precipitation",
    ]
    df_all = pd.DataFrame(columns=all_columns)
    try:
        with tarfile.open(fname, "r:gz") as tar:
            tar.extractall(path=dest)  # Extracts all files to the specified path
    except tarfile.TarError as e:
        print(f"An error occurred: {e}")
    except FileNotFoundError:
        print(f"Error: The file {fname} was not found.")

    file = os.path.join(dest, "u_wind_d2.nc")
    ds_u = xr.open_dataset(file)
    file = os.path.join(dest, "v_wind_d2.nc")
    ds_v = xr.open_dataset(file)
    ds = xr.merge([ds_u, ds_v], join="outer")

    df_nwp = []
    for _, idf in df_stn.iterrows():
        lat = idf["latitude"]
        lon = idf["longitude"]
        plant_id = idf["id"]
        df_temp = ds.sel(lat=lat, lon=lon, method="nearest").to_dataframe()
        df_temp = df_temp.reset_index()
        df_temp["plant_id"] = plant_id
        df_nwp.append(df_temp)
    df_nwp = pd.concat(df_nwp)
    df_nwp["wind_speed"] = np.sqrt(df_nwp["u"] ** 2 + df_nwp["v"] ** 2)
    df_nwp["wind_direction"] = (
        np.degrees(np.arctan2(df_nwp["u"], df_nwp["v"])) + 180
    ) % 360
    df_nwp["model_name"] = "ncm_d2"
    df_nwp = df_nwp.rename({"time": "forecast_time", "lev": "height"}, axis=1)
    df_nwp["prediction_time"] = df_nwp["forecast_time"].min()
    df_all = pd.concat([df_all, df_nwp])
    df_all = df_all[all_columns]
    return df_all
