import traceback
import requests
from typing import List, Dict, Any
from datetime import datetime, timezone
import os
import yaml
import pandas as pd
import traceback
import utils
from datetime import datetime as dt
from urllib.parse import quote as urlquote
from sqlalchemy import create_engine
import numpy as np


CONFIG_PATH = os.getenv("WEATHER_CONFIG", "reinsight_config.yml")
with open(CONFIG_PATH, "r") as f:
    config = yaml.safe_load(f)

db_columns = config["db_columns"]
weather_table_column = db_columns["weather_table"]["columns"]
weather_table_column_un = db_columns["weather_table"]["unique_constraint"]

SCRIPT_NAME = os.path.basename(__file__)
APIM_KEY = config["openmeteo_key_jade"]
HEADERS  = {'Ocp-Apim-Subscription-Key': APIM_KEY}
GATEWAY="https://jbsfcstplapimgatewaydev.azure-api.net/weather"

def latest_prediction_time(model):
    '''
    ecmwf_ifs,ecmwf_ifs025,gfs_global,icon_global,ukmo_global_deterministic_10km,meteofrance_arpege_world,bom_access_global
    '''
    if model == "gfs_global":
        model = "ncep_gfs025"
    if model == "meteofrance_arpege_world":
        model = "meteofrance_arpege_world025"
    if model == "icon_global":
        model = "dwd_icon"
    url = f"https://customer-api.open-meteo.com/data/{model}/static/meta.json"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        timestamp = data.get("last_run_initialisation_time")
        if timestamp is None:
            return "Key 'last_run_initialisation_time' not found."
        
        # Convert Unix timestamp to UTC datetime object
        dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
        
        # Format to human-readable string
        return dt
    except requests.exceptions.RequestException as e:
        print(f"An error occurred while fetching data from Open-Meteo: {e}")
        return {}
    
def get_wind_and_cloud_forecast(latitudes: List[float], longitudes: List[float], model: str, forecast_days: int = 3) -> Dict[str, Any]:
    """
    Fetches wind and cloud cover forecast data from the Open-Meteo API 
    for a given list of latitudes and longitudes.
    
    :param latitudes: List of latitude floats (e.g., [52.52, 50.12])
    :param longitudes: List of longitude floats (e.g., [13.41, 8.68])
    :return: Dictionary containing the JSON response from the API
    """
    if len(latitudes) != len(longitudes):
        raise ValueError("The number of latitudes and longitudes must match.")
    
    # Base URL for the customer/commercial API as requested
    base_url = "https://customer-api.open-meteo.com/v1/forecast"
    
    # Convert lists of coordinates into comma-separated strings
    lat_string = ",".join(map(str, latitudes))
    lon_string = ",".join(map(str, longitudes))
    
    # Define the query parameters exactly matching your requested URL
    params = {
        "latitude": lat_string,
        "longitude": lon_string,
        "hourly": (
            "wind_speed_10m,wind_speed_80m,wind_speed_120m,"
            "wind_direction_10m,wind_direction_80m,wind_direction_120m,"
            "cloud_cover,cloud_cover_low,cloud_cover_mid,cloud_cover_high,"
            "shortwave_radiation"
        ),
        "models": model,
        "forecast_days": forecast_days,
        "wind_speed_unit": "ms",
        "apikey": "rjlUQOn5yR5RbGPH",
    }
    
    try:
        response = requests.get(base_url, params=params)
        # Raise an exception for 4xx or 5xx status codes
        response.raise_for_status() 
        return response.json()
    
    except requests.exceptions.RequestException as e:
        print(f"An error occurred while fetching data from Open-Meteo: {e}")
        return {}


def get_live_forecast(lats, lons, forecast_days, model):
    if isinstance(lats, (int, float)):
        lats = [lats]
    if isinstance(lons, (int, float)):
        lons = [lons]
        
    # Open-Meteo expects comma-separated strings for multiple coordinates
    lat_string = ",".join(map(str, lats))
    lon_string = ",".join(map(str, lons))

    model_params = [
            "wind_speed_10m,wind_speed_80m,wind_speed_120m,"
            "wind_direction_10m,wind_direction_80m,wind_direction_120m,"
            "cloud_cover,cloud_cover_low,cloud_cover_mid,cloud_cover_high,"
            "shortwave_radiation"
        ]
    model_params = model_params[0]
    r = requests.get(
        f'{GATEWAY}/v1/forecast',
        params={
            'latitude':     lat_string,
            'longitude':    lon_string,
            'hourly':       model_params,
            'models': model,
            'forecast_days': forecast_days,
            'wind_speed_unit': 'ms'
        },
        headers=HEADERS
    )
    print(f'Status: {r.status_code}')
    out = r.json()
    return out


if __name__ == "__main__":
    db_cred = config["db_cred"]
    engine = create_engine(
    f"postgresql://{db_cred['user_name']}:%s@{db_cred['user_ip']}:{db_cred['user_port']}/{db_cred['db_name']}"
        % urlquote(db_cred["user_passwd"])
    )

    
    db_columns = config["db_columns"]
    fct_table_column = db_columns["forecast_table"]["columns"]
    fct_table_column_un = db_columns["forecast_table"]["unique_constraint"]
    db_con = utils.DBcon(con = engine, db_schema=db_columns)
    db_con.logging({"script": SCRIPT_NAME, "log_type": "info", "message": f"Openmeteo data fetch script started"})

    df_static = db_con.get_static_data()
    df_static = df_static[df_static["parent_id"] != 0]

    lats = df_static["latitude"].tolist()
    lons = df_static["longitude"].tolist()
    
    models = ["ecmwf_ifs","ecmwf_ifs025","gfs_global","icon_global","ukmo_global_deterministic_10km","meteofrance_arpege_world"]

    OPENMETEO_MODEL_MANIFEST = "../data_lake/re_insights/manifest_files/openmeteo_model_manifest.csv"
    if os.path.exists(OPENMETEO_MODEL_MANIFEST):
        df_manifest = pd.read_csv(OPENMETEO_MODEL_MANIFEST, index_col=0)
    else:
        df_manifest = pd.DataFrame(columns=models, index=[0])

    for model in models:
        try:
            if model in df_manifest.columns:
                existing_prediction_time = df_manifest[model].dropna().index.tolist()
                existing_prediction_time = [x.strftime("%Y%m%d%H") if isinstance(x, dt) else str(x) for x in existing_prediction_time]
            else:
                existing_prediction_time = []
            
            prediction_time = latest_prediction_time(model)
            prediction_time_str = prediction_time.strftime("%Y%m%d%H")
            if prediction_time_str in existing_prediction_time:
                print(f"Exists {model}")
                continue
            try:
                forecast_data = get_live_forecast(lats, lons, model = model, forecast_days=1)
            except Exception as e:
                db_con.logging({"script": model, "log_type": "error", "message": f"Openmeteo data fetch script failed for {model}: {e}"})
                continue
            
            all_locations = []
            # Loop through each item in the JSON list
            for i, item in enumerate(forecast_data):
                # 1. Extract the hourly weather metrics dictionary
                hourly_dict = item['hourly']
                
                # 2. Convert the hourly dictionary directly into a temporary DataFrame
                df_hourly = pd.DataFrame(hourly_dict)
            
                # 3. Add the location's metadata to every row in this temporary DataFrame
                df_hourly['latitude'] = lats[i]
                df_hourly['longitude'] = lons[i]

                # Use .get() for location_id since the first location doesn't have it
                df_hourly['location_id'] = item.get('location_id', None) 
                
                all_locations.append(df_hourly)

            # Combine all individual location dataframes into one master DataFrame
            final_df = pd.concat(all_locations, ignore_index=True)


            # 1. Unpivot both wind_speed and wind_direction columns by height
            df_long = pd.wide_to_long(
                final_df, 
                stubnames=['wind_speed', 'wind_direction'], 
                i=['time', 'latitude', 'longitude', 'cloud_cover', 'cloud_cover_low', 'cloud_cover_mid', 'cloud_cover_high', 'shortwave_radiation'], 
                j='height', 
                sep='_', 
                suffix=r'\d+m'
            ).reset_index()

            # 2. Clean up the 'height' column values (e.g., '10m' -> 10)
            df_long['height'] = df_long['height'].str.replace('m', '').astype(int)

            # 3. Rename columns to match your target schema
            df_long = df_long.rename(columns={
                'time': 'forecast_time',
                'cloud_cover_low': 'low_cloud',
                'cloud_cover_mid': 'medium_cloud',
                'cloud_cover_high': 'high_cloud',
                'cloud_cover': 'total_cloud',
                'shortwave_radiation': 'ghi'
            })

            # 4. Clear out cloud and radiation metrics only for rows that are NOT 10m
            cloud_and_ghi_cols = ['low_cloud', 'medium_cloud', 'high_cloud', 'total_cloud', 'ghi']
            df_long.loc[df_long['height'] != 10, cloud_and_ghi_cols] = np.nan

            # 5. Populate placeholder columns for the rest of your schema features
            for col in ['humidity', 'temperature', 'precipitation']:
                df_long[col] = np.nan

            # 6. Arrange columns in your desired schema layout
            final_cols = [
                'forecast_time', 'latitude', 'longitude', 'height', 
                'wind_speed', 'wind_direction', 'low_cloud', 'medium_cloud', 
                'high_cloud', 'total_cloud', 'ghi', 'humidity', 'temperature', 'precipitation'
            ]
            df_target = df_long[final_cols]

            df_target["model_name"] = model
            df_target['forecast_time'] = pd.to_datetime(df_target['forecast_time'])
            df_target["prediction_time"] = df_target["forecast_time"].dt.floor('6h')
            df_target['prediction_time'] = df_target['prediction_time'].dt.tz_localize('UTC')
            df_target['forecast_time'] = df_target['forecast_time'].dt.tz_localize('UTC')

            df_target = pd.merge(df_target, df_static, on=["latitude", "longitude"])

            df_target = df_target[db_columns["weather_table"]["columns"]]
            df_target = df_target.set_index(db_columns["weather_table"]["unique_constraint"])
            db_con.push_weather_data(df_target)

            data_filepath = f"../data_lake/re_insights/openmeteo/data/{prediction_time.strftime('%Y%m%d_%H%M%S')}_{model}_fct.csv"
            df_target.to_csv(data_filepath)
            df_manifest.loc[prediction_time_str, model] = 1
            db_con.logging({"script": model, "log_type": "success", "message": f"Openmeteo data fetch script completed for {model}, prediction time: {prediction_time_str}"})
        
        except Exception as e:
            e = traceback.format_exc()
            print(e)
            db_con.logging({"script": model, "log_type": "error", "message": f"Openmeteo data fetch script failed for {model}: {e}"})
            continue
df_manifest.to_csv(OPENMETEO_MODEL_MANIFEST)
db_con.logging({"script": SCRIPT_NAME, "log_type": "info", "message": f"Openmeteo data fetch script completed"})
