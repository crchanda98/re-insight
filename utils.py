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


class APICon:
    def __init__(self, base_url="http://127.0.0.1:5000", config_path=None):
        self.base_url = base_url.rstrip("/")
        
        # Load config if path provided or from environment variable
        self.config_path = config_path or os.getenv("WEATHER_CONFIG", "reinsight_config.yml")
        try:
            with open(self.config_path, "r") as f:
                self.config = yaml.safe_load(f)
        except Exception as e:
            print(f"Warning: Could not load config file at {self.config_path}: {e}")
            self.config = {}

    def _post_data(self, endpoint, payload):
        """Helper to post payload to the API and return the response JSON."""
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        print(f"Pushing {len(payload['data'])} records to {url}...")
        
        try:
            response = requests.post(url, json=payload)
            print(f"Status Code: {response.status_code}")
            
            try:
                res_json = response.json()
                return res_json
            except Exception:
                return {"error": "Failed to parse JSON response", "context": response.text}
                
        except Exception as e:
            return {"error": str(e)}

    def upload_static_data(self, df):
        """Uploads static plant data, handling JSON string fields and missing values."""
        print("\n--- Uploading Static Data ---")
        try:
            # Drop rows missing the unique identifier
            if "plant_name" in df.columns:
                df = df.dropna(subset=["plant_name"])
            
            # Replace NaNs with None for JSON
            df = df.replace({np.nan: None})
            
            # Pre-process malformed JSON like "{turbine: 100}" to "{\"turbine\": 100}"
            def fix_json_quotes(val):
                if isinstance(val, str) and (val.startswith("{") or val.startswith("[")):
                    val = re.sub(r'([{,]\s*)([A-Za-z0-9_]+)(\s*:)', r'\1"\2"\3', val)
                    val = val.replace("'", '"')
                return val

            # Parse stringified lists/dicts
            for col in df.columns:
                if df[col].dtype == object:
                    try:
                        df[col] = df[col].apply(fix_json_quotes)
                        df[col] = df[col].apply(
                            lambda x: ast.literal_eval(x) if isinstance(x, str) and (x.startswith("[") or x.startswith("{")) else x
                        )
                    except Exception:
                        pass
                        
            # Specific handling for regulation_bands
            if "regulation_bands" in df.columns:
                def convert_to_float_list(val):
                    if val is None: return None
                    if isinstance(val, (list, tuple)): return [float(v) for v in val]
                    if isinstance(val, (int, float)): return [float(val)]
                    if isinstance(val, str):
                        try:
                            parsed = json.loads(val.replace("'", '"'))
                            if isinstance(parsed, list): return [float(v) for v in parsed]
                            return [float(parsed)]
                        except Exception:
                            pass
                    return val
                df["regulation_bands"] = df["regulation_bands"].apply(convert_to_float_list)

            payload = {"data": df.to_dict(orient="records")}
            return self._post_data("/static_table/push", payload)
            
        except Exception as e:
            return {"error": f"An error occurred uploading static data: {e}"}

    def upload_weather_data(self, df):
        """Uploads weather timeseries data, stripping index columns and NaNs."""
        print("\n--- Uploading Weather Data ---")
        try:
            if "Unnamed: 0" in df.columns:
                df = df.drop(columns=["Unnamed: 0"])

            df = df.replace({np.nan: None})

            payload = {"data": df.to_dict(orient="records")}
            return self._post_data("/weather/push", payload)

        except Exception as e:
            return {"error": f"An error occurred uploading weather data: {e}"}

    def fetch_static_data(self):
        """Fetches all static data from the API."""
        url = f"{self.base_url}/static_table/all"
        print(f"\n--- Fetching Static Data from {url} ---")
        try:
            response = requests.get(url)
            print(f"Status Code: {response.status_code}")
            if response.status_code == 200:
                data = response.json()
                print(f"Successfully fetched {len(data)} static records.")
                return data
            else:
                print("Response Context:", response.text)
                return None
        except Exception as e:
            print(f"Failed to fetch static data: {e}")
            return None

    def fetch_weather_data(self, plant_name, model_name=None, start_date=None, end_date=None):
        """Fetches weather data for a specific plant_name, optionally filtered by model_name and date."""
        url = f"{self.base_url}/weather/pull/{plant_name}"
        params = []
        if model_name:
            params.append(f"model_name={model_name}")
        if start_date:
            params.append(f"start_date={start_date}")
        if end_date:
            params.append(f"end_date={end_date}")
            
        if params:
            url += "?" + "&".join(params)
            
        print(f"\n--- Fetching Weather Data from {url} ---")
        try:
            response = requests.get(url)
            print(f"Status Code: {response.status_code}")
            if response.status_code == 200:
                data = response.json()
                print(f"Successfully fetched {len(data)} weather records.")
                return data
            else:
                print("Response Context:", response.text)
                return None
        except Exception as e:
            print(f"Failed to fetch weather data: {e}")
            return None



class SendTeleMsg:
    def __init__(self):
        self.api_url = None
        self.channels = None
        if isinstance(self.channels, str):
            self.channels = [self.channels]

    def send_text(self, imsg, image_bytes=None, ich=None):
        if image_bytes is None:
            if isinstance(self.channels, list):
                for ich in self.channels:
                    base_url = f"{self.api_url}/sendMessage"
                    params = {
                        "chat_id": -ich,
                        "text": imsg,
                    }
                    requests.get(base_url, params=params, timeout=5)
            else:
                base_url = f"{self.api_url}/sendMessage"
                params = {
                    "chat_id": ich,
                    "text": imsg,
                }
                requests.get(base_url, params=params, timeout=5)
        else:
            if isinstance(self.channels, list):
                for ich in self.channels:
                    base_url = f"{self.api_url}/sendPhoto"
                    files = {"photo": image_bytes}
                    data = {"chat_id": -ich, "caption": imsg, "parse_mode": "Markdown"}
                    requests.post(base_url, files=files, data=data, timeout=10)
            else:
                base_url = f"{self.api_url}/sendPhoto"
                files = {"photo": image_bytes}
                data = {"chat_id": -ich, "caption": imsg, "parse_mode": "Markdown"}
                requests.post(base_url, files=files, data=data, timeout=10)

