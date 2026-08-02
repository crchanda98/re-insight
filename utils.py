import requests
from requests.auth import HTTPBasicAuth
import pandas as pd
import numpy as np
import xarray as xr
import tarfile
import os
from tqdm import tqdm
from datetime import datetime as dt, timedelta, timezone
import yaml
from pangres import upsert
import traceback
import glob
from pathlib import Path
from ftplib import FTP
import subprocess

def push_fct_to_ftp(filename, FTP_HOST, FTP_USER, FTP_PASS):
    base_name = os.path.basename(filename)
    with FTP(FTP_HOST) as ftp:
        ftp.login(user=FTP_USER, passwd=FTP_PASS)
        ftp.cwd("/home/ftpuser/ftp/upload")
        with open(filename, "rb") as file:
            ftp.storbinary(f"STOR {base_name}", file)

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
    uwind_file = glob.glob(dest + "/u*2.nc")[0]
    ds_u = xr.open_dataset(uwind_file)
    vwind_file = glob.glob(dest + "/v*2.nc")[0]
    ds_v = xr.open_dataset(vwind_file)
    ds = xr.merge([ds_u, ds_v], join="outer")

    df_nwp = []
    for _, idf in df_stn.iterrows():
        lat = idf["latitude"]
        lon = idf["longitude"]
        plant_id = idf["plant_id"]
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
    df_nwp = df_nwp[df_nwp["lev"].isin([50, 80])]
    df_nwp = df_nwp.rename({"time": "forecast_time", "lev": "height"}, axis=1)
    df_nwp['forecast_time'] = pd.to_datetime(df_nwp['forecast_time'])
    df_nwp["prediction_time"] = df_nwp["forecast_time"].min()
    df_all = pd.concat([df_all, df_nwp])
    df_all = df_all[all_columns]
    df_all = df_all.dropna(how = "all")
    # os.system(f"rm -rf {dest}/*nc")
    ds_u.close() # Manually close the handle
    ds_v.close() # Manually close the handle
    dest_path = Path(dest)
    for nc_file in dest_path.glob("*.nc"):
        nc_file.unlink()
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
        url = f"{self.base_url}/static_table/pull"
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


    def fetch_meas_data(self, plant_name, start_date=None, end_date=None):
        """Fetches weather data for a specific plant_name, optionally filtered by model_name and date."""
        url = f"{self.base_url}/meas/pull/{plant_name}"
        params = []
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


def get_last_15_min_slot(dt_now=dt.now()):
    minute = (dt_now.minute // 15) * 15
    return dt_now.replace(minute=minute, second=0, microsecond=0)

class DBcon:
    def __init__(self, con, db_schema, schma_name = "re_insight"):
        self.db_schema = db_schema
        self.conn = con
        self.schma_name = schma_name
        self.df_static = pd.read_sql("select * from re_insight.static_table", con=self.conn)
    
    def get_static_data(self):
        return self.df_static
    
    def push_static_data(self, idf):
        if "plant_name" in idf.columns:
            idf = idf.set_index("plant_name")
        upsert(
            con=self.conn,
            df=idf,
            table_name="static_table",
            schema=self.schma_name,
            if_row_exists="update",
        )
    
    def get_weather_data(self, plant, model, start_date, end_date, print_query = False):
        ist = self.df_static[self.df_static["plant_name"] == plant].iloc[0]
        query = f"select * from re_insight.weather_table \
            where plant_id = {ist['plant_id']} \
            and model_name = '{model}' \
            and forecast_time between '{start_date}' and '{end_date}'"
        if print_query:
            print(query)
        df_weather = pd.read_sql(query, con=self.conn)
        df_weather["plant_name"] = ist["plant_name"]
        return df_weather
    
    def push_weather_data(self, idf):
        upsert(
            con=self.conn,
            df=idf,
            table_name="weather_table",
            schema="re_insight",
            if_row_exists="update",
        )
    
    def get_meas_data(self, plant, start_date, end_date, print_query = False):
        ist = self.df_static[self.df_static["plant_name"] == plant].iloc[0]
        query = f"select * from re_insight.meas_table \
            where plant_id = {ist['plant_id']} \
            and record_time between '{start_date}' and '{end_date}'"
        if print_query:
            print(query)
        df_weather = pd.read_sql(query, con=self.conn)
        df_weather["plant_name"] = ist["plant_name"]
        return df_weather

    def push_meas_data(self, idf):
        upsert(
            con=self.conn,
            df=idf,
            table_name="meas_table",
            schema="re_insight",
            if_row_exists="update",
        )

    def get_fct_data(self, plant, fct_src, model_name, start_date, end_date):
        ist = self.df_static[self.df_static["plant_name"] == plant].iloc[0]
        df_weather = pd.read_sql(f"select * from re_insight.forecast_table \
            where plant_id = {ist['plant_id']} \
            and forecast_source = '{fct_src}' \
            and model_name = '{model_name}' \
            and forecast_time between '{start_date}' and '{end_date}'", con=self.conn)
        df_weather["plant_name"] = ist["plant_name"]
        return df_weather

    def push_fct_data(self, idf):
        upsert(
            con=self.conn,
            df=idf,
            table_name="forecast_table",
            schema="re_insight",
            if_row_exists="update",
        )
    
    def get_log_data(self, script, start_date, end_date):
        df_log = pd.read_sql(f"select * from re_insight.logging_table where script = '{script}' \
            and created_at between '{start_date} 00:00:00' and '{end_date} 23:59:59' order by created_at", con=self.conn)
        return df_log
    
    def push_log_data(self, idf):
        idf = idf.sort_values(["script", "logging_time", "log_type"])
        idf = idf.drop_duplicates(subset=['logging_time', 'script', 'log_type'], keep='last')
        idf = idf.set_index(["script", "logging_time", "log_type"])
        upsert(
            con=self.conn,
            df=idf,
            table_name="logging_table",
            schema="re_insight",
            if_row_exists="update",
        )

    def logging(self, log_dict):
        if "logging_time" not in log_dict:
            log_dict["logging_time"] = dt.now(timezone.utc)
        df_log = pd.DataFrame([log_dict])
        self.push_log_data(df_log)

def get_openmeteo(lat, lon):
    url = "https://customer-api.open-meteo.com/v1/forecast?latitude=52.52&longitude=13.41&hourly=wind_speed_80m,wind_speed_120m&models=ecmwf_ifs,gfs_global,icon_global,meteofrance_arpege_world,gem_global&forecast_days=1&wind_speed_unit=ms&apikey=rjlUQOn5yR5RbGPH"
    resp = requests.get(url)
    out = resp.json()
    return out




def calculate_mp_dsm_directional_losses(actual, forecast, avc_kw, ppa_rate=5.0):
    """
    Calculates summary strict-DSM metrics for Andhra Pradesh (APERC) Regulations.
    """
    # Force numpy arrays for vectorized stability across Series/Arrays
    actual = np.array(actual)
    forecast = np.array(forecast)
    
    # 1. Energy Calculation (Power kW -> Energy kWh for 15-min block)
    actual_kwh = actual / 4
    forecast_kwh = forecast / 4
    avc_kwh = avc_kw / 4
    
    gross_revenue = actual_kwh * ppa_rate
    
    # 2. Forecast Error % (Normalized against AvC)
    error_pct = np.where(avc_kw > 0, (np.abs(actual - forecast) / avc_kw) * 100, 0.0)
    
    # 3. APERC Progressive Slab Step Breakdown
    slab1_err = np.minimum(error_pct, 15)
    slab2_err = np.minimum(np.maximum(error_pct - 15, 0), 10)
    slab3_err = np.minimum(np.maximum(error_pct - 25, 0), 10)
    slab4_err = np.maximum(error_pct - 35, 0)
    
    # 4. Convert Slab Percentages to Slab Energies (kWh)
    slab2_energy = (slab2_err / 100) * avc_kwh
    slab3_energy = (slab3_err / 100) * avc_kwh
    slab4_energy = (slab4_err / 100) * avc_kwh
    
    # 5. Regulatory DSM Penalty (APERC Flat Rates)
    total_dsm_penalty_rs = (slab2_energy * 0.50) + (slab3_energy * 1.00) + (slab4_energy * 1.50)
    
    # 6. Directional Separation of the Strict DSM Penalty
    dsm_over_rs = np.where(actual_kwh > forecast_kwh, total_dsm_penalty_rs, 0.0)
    dsm_under_rs = np.where(actual_kwh < forecast_kwh, total_dsm_penalty_rs, 0.0)
    
    # 7. Aggregate Metrics Cleanup & Return Dictionary
    actual_sum = float(actual_kwh.sum())
    penalty_sum = float(total_dsm_penalty_rs.sum())
    revenue_sum = float(gross_revenue.sum())
    
    return {
        'Actual_Generation_kWh': actual_sum,
        'Target_Generation_kWh': float(forecast_kwh.sum()),
        'Gross_Revenue_Rs': revenue_sum,
        'Over_Injection_DSM_Penalty_Rs': float(dsm_over_rs.sum()),
        'Under_Injection_DSM_Penalty_Rs': float(dsm_under_rs.sum()),
        'Total_Statutory_DSM_Penalty_Rs': penalty_sum,
        'DSM_Impact_Rs_per_Actual_kWh': penalty_sum / actual_sum if actual_sum > 0 else 0.0,
        'Total_loss_percentage': (penalty_sum / revenue_sum) * 100 if revenue_sum > 0 else 0.0
    }

def calculate_ap_dsm_directional_losses(actual, forecast, avc_kw=84000, ppa_rate=5.0):
    actual_kwh = actual / 4
    forecast_kwh = forecast / 4
    avc_kwh = avc_kw / 4
    gross_revenue = actual_kwh * ppa_rate
    error_pct = np.where(avc_kw > 0, (np.abs(actual - forecast) / avc_kw) * 100, 0.0)
    
    slab1_err = np.minimum(error_pct, 15)
    slab2_err = np.minimum(np.maximum(error_pct - 15, 0), 10)
    slab3_err = np.minimum(np.maximum(error_pct - 25, 0), 10)
    slab4_err = np.maximum(error_pct - 35, 0)
    
    slab2_energy = (slab2_err / 100) * avc_kwh
    slab3_energy = (slab3_err / 100) * avc_kwh
    slab4_energy = (slab4_err / 100) * avc_kwh
    
    total_dsm_penalty_rs = (slab2_energy * 0.50) + (slab3_energy * 1.00) + (slab4_energy * 1.50)
    
    dsm_over_rs = np.where(actual_kwh > forecast_kwh, total_dsm_penalty_rs, 0.0)
    dsm_under_rs = np.where(actual_kwh < forecast_kwh, total_dsm_penalty_rs, 0.0)
    
    return {
        'Actual_Generation_kWh': actual_kwh.sum(),
        'Target_Generation_kWh': forecast_kwh.sum(),
        'Gross_Revenue_Rs': actual_kwh.sum() * ppa_rate,
        'Over_Injection_DSM_Penalty_Rs': dsm_over_rs.sum(),
        'Under_Injection_DSM_Penalty_Rs': dsm_under_rs.sum(),
        'Total_Statutory_DSM_Penalty_Rs': total_dsm_penalty_rs.sum(),
        'DSM_Impact_Rs_per_Actual_kWh': total_dsm_penalty_rs.sum() / actual_kwh.sum() if actual_kwh.sum() > 0 else 0.0,
        'Total_loss_percentage': (total_dsm_penalty_rs.sum() / gross_revenue.sum()) * 100
    }

def filter_forecast_by_regulation(dfp, regulation="CTU", lag_hours=2):
    dfp = dfp.copy()
    dfp = dfp.sort_values(by=["prediction_time", "forecast_time"])
    if regulation == "CTU":
        lag_mask = (dfp["forecast_time"] - dfp["prediction_time"]) >= pd.Timedelta(hours=2)
        dfp = dfp[lag_mask]

    elif regulation == "CTU-RTM":
        lag_mask = (dfp["forecast_time"] - dfp["prediction_time"]) >= pd.Timedelta(hours=1)
        dfp = dfp[lag_mask]
    
    elif regulation == "MP":
        pred_minutes_from_midnight = dfp['prediction_time'].dt.hour * 60 + dfp['prediction_time'].dt.minute
        dfp_filter = dfp[(pred_minutes_from_midnight - 1350) % 90 == 0].copy()

        dfp_filter['block_start'] = dfp_filter['prediction_time'] + pd.Timedelta(minutes=90)
        dfp_filter['block_end'] = dfp_filter['block_start'] + pd.Timedelta(minutes=75)

        dfp_filter = dfp_filter[
            (dfp_filter['forecast_time'] >= dfp_filter['block_start']) & 
            (dfp_filter['forecast_time'] <= dfp_filter['block_end'])
        ].copy()

        dfp_filter = dfp_filter.drop(columns=['block_start', 'block_end']).sort_values(by='forecast_time')
        dfp = dfp_filter.reset_index(drop=True)

    else:
        dfp["prediction_time"] = pd.to_datetime(dfp["prediction_time"], utc=True).dt.tz_convert("Asia/Kolkata").dt.tz_localize(None)
        lag_mask = (dfp["forecast_time"] - dfp["prediction_time"]) == pd.Timedelta(hours=lag_hours)
        dfp = dfp[lag_mask]
        
    dfp = dfp.sort_values(by=["forecast_time", "prediction_time"])
    dfp = dfp.drop_duplicates(subset=["forecast_time"], keep="last")
    return dfp

def assign_global_and_regional_grids(df, global_boxes, regional_boxes, lat_col='latitude', lon_col='longitude'):
    """
    Vectorized function to match rows against both Global and Regional grids.
    Assigns None (null) independently if a point falls outside either set.
    """
    df_out = df.copy()
    
    # Initialize both tracking columns with None (null values)
    df_out['assigned_global_grid'] = None
    df_out['assigned_regional_grid'] = None
    
    # 1. Process Global Boxes
    for box in global_boxes:
        mask_global = (
            df_out['assigned_global_grid'].isna() & 
            (df_out[lon_col] >= box['lon_min']) & (df_out[lon_col] <= box['lon_max']) &
            (df_out[lat_col] >= box['lat_min']) & (df_out[lat_col] <= box['lat_max'])
        )
        df_out.loc[mask_global, 'assigned_global_grid'] = box['grid_id']
        
    # 2. Process Regional Boxes
    for box in regional_boxes:
        mask_regional = (
            df_out['assigned_regional_grid'].isna() & 
            (df_out[lon_col] >= box['lon_min']) & (df_out[lon_col] <= box['lon_max']) &
            (df_out[lat_col] >= box['lat_min']) & (df_out[lat_col] <= box['lat_max'])
        )
        df_out.loc[mask_regional, 'assigned_regional_grid'] = box['grid_id']
        
    return df_out

def extract_ncm_ad(fname, dest, df_stn, zone):

    # Setup separate grid definitions (giving them distinct names to tell them apart)
    global_definitions = [
        {"grid_id": 1, "lon_min": 77.0, "lon_max": 83.5, "lat_min": 18.75, "lat_max": 28.5},
        {"grid_id": 2, "lon_min": 68.0, "lon_max": 77.0, "lat_min": 20.5,  "lat_max": 31.0},
        {"grid_id": 3, "lon_min": 75.0, "lon_max": 80.0, "lat_min": 8.0,   "lat_max": 18.75},
    ]

    regional_definitions = [
        {"grid_id": 1, "lon_min": 77.0, "lon_max": 83.5, "lat_min": 18.75, "lat_max": 28.5},
        {"grid_id": 2, "lon_min": 68.0, "lon_max": 77.0, "lat_min": 20.5,  "lat_max": 31.0},
        {"grid_id": 3, "lon_min": 75.0, "lon_max": 80.0, "lat_min": 8.0,   "lat_max": 18.75},
    ]

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
    
    df_stn_with_box = assign_global_and_regional_grids(
        df_stn, 
        global_boxes=global_definitions, 
        regional_boxes=regional_definitions,
    )

    df_nwp = []
    dest_path = Path(dest)
    for region in range(1,4):
        if zone == "ncum_g":
            df_st = df_stn_with_box[df_stn_with_box["assigned_regional_grid"] == region]
            model_name = "ncum_g"
            nc_file = os.path.join(dest, f"u_wind_R{region}.nc")
            ds_u = xr.open_dataset(nc_file)
            nc_file = os.path.join(dest, f"v_wind_R{region}.nc")
            ds_v = xr.open_dataset(nc_file)
            nc_file = os.path.join(dest, f"solarradiation_15m_R{region}.nc")
            ds_ghi = xr.open_dataset(nc_file)
            ds_speed = xr.merge([ds_u, ds_v], join="outer")
            ds_surface = xr.merge([ds_ghi], join="outer")
            ds_comb = xr.merge([ds_speed, ds_surface], join = "inner")

        if zone == "ncum_r":
            df_st = df_stn_with_box[df_stn_with_box["assigned_global_grid"] == region]
            model_name = "ncum_r"
            nc_file = os.path.join(dest, f"solar_radiation_R{region}.nc")
            ds_ghi = xr.open_dataset(nc_file)
            ds_surface = ds_ghi.copy()
            ds_ghi.close()
        
        if len(df_st) == 0:
            continue
        
        for _, idf in df_st.iterrows():
            lat = idf["latitude"]
            lon = idf["longitude"]
            plant_id = idf["plant_id"]
            if zone == "ncum_g":
                df_temp_speed = ds_speed.sel(lat=lat, lon=lon, method="nearest").to_dataframe().reset_index()
                df_temp_surf = ds_surface.sel(lat=lat, lon=lon, method="nearest").to_dataframe().reset_index()
                df_temp_surf["lev"] = 0
                df_temp = pd.merge(df_temp_speed, df_temp_surf, on=["time", "lon", "lat", "lev"], how="outer")
            
            if zone == "ncum_r":
                df_temp = ds_surface.sel(lat=lat, lon=lon, method="nearest").to_dataframe().reset_index()
                df_temp["lev"] = 0

            df_temp["plant_id"] = plant_id
            df_nwp.append(df_temp)
        
    df_nwp = pd.concat(df_nwp)
    if zone == "ncum_g":
        df_nwp["wind_speed"] = np.sqrt(df_nwp["u"] ** 2 + df_nwp["v"] ** 2)
        df_nwp["wind_direction"] = (
            np.degrees(np.arctan2(df_nwp["u"], df_nwp["v"])) + 180
        ) % 360
        df_nwp = df_nwp[df_nwp["lev"].isin([0, 50, 80, 100.0, 120.0])]
    
    if zone == "ncum_r":
        df_nwp["lev"] = 0
    
    df_nwp["model_name"] = model_name
    df_nwp = df_nwp.rename({"time": "forecast_time", "lev": "height", "dswrf": "ghi"}, axis=1)
    df_nwp['forecast_time'] = pd.to_datetime(df_nwp['forecast_time'])
    df_nwp["prediction_time"] = df_nwp["forecast_time"].min()
    df_nwp["prediction_time"] = df_nwp["prediction_time"].dt.floor('6h')
    df_nwp['prediction_time'] = df_nwp['prediction_time'].dt.tz_localize('UTC')
    df_nwp['forecast_time'] = df_nwp['forecast_time'].dt.tz_localize('UTC')
    df_all = pd.concat([df_all, df_nwp])
    df_all = df_all[all_columns]
    df_all = df_all.dropna(how = "all")
    return df_all