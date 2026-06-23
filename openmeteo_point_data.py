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


CONFIG_PATH = os.getenv("WEATHER_CONFIG", "reinsight_config.yml")
with open(CONFIG_PATH, "r") as f:
    config = yaml.safe_load(f)

SCRIPT_NAME = os.path.basename(__file__)

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
    
    models = ["ecmwf_ifs","ecmwf_ifs025","gfs_global","icon_global","ukmo_global_deterministic_10km","meteofrance_arpege_world","bom_access_global"]
    models = ["ecmwf_ifs","ecmwf_ifs025"]

    OPENMETEO_MODEL_MANIFEST = "../data_lake/re_insights/openmeteo/openmeteo_model_manifest.csv"
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
                # continue
            try:
                forecast_data = get_wind_and_cloud_forecast(lats, lons, model = model, forecast_days=1)
            except Exception as e:
                db_con.logging({"script": model, "log_type": "error", "message": f"Openmeteo data fetch script failed for {model}: {e}"})
                continue
            
            all_locations = []
            # Loop through each item in the JSON list
            for item in forecast_data:
                # 1. Extract the hourly weather metrics dictionary
                hourly_dict = item['hourly']
                
                # 2. Convert the hourly dictionary directly into a temporary DataFrame
                df_hourly = pd.DataFrame(hourly_dict)
            
                # 3. Add the location's metadata to every row in this temporary DataFrame
                df_hourly['latitude'] = item['latitude']
                df_hourly['longitude'] = item['longitude']

                # Use .get() for location_id since the first location doesn't have it
                df_hourly['location_id'] = item.get('location_id', None) 
                
                all_locations.append(df_hourly)

            # Combine all individual location dataframes into one master DataFrame
            final_df = pd.concat(all_locations, ignore_index=True)

            # Optional: Reorder columns to put metadata on the left
            metadata_cols = ['location_id', 'latitude', 'longitude', 'time']
            other_cols = [col for col in final_df.columns if col not in metadata_cols]
            final_df = final_df[metadata_cols + other_cols]
            final_df["prediction_time_utc"] = prediction_time
            # final_df = final_df.merge(df_static, on=["latitude", "longitude"], how="left")
            data_filepath = f"../data_lake/re_insights/openmeteo/data/{prediction_time.strftime('%Y%m%d_%H%M%S')}_{model}_fct.csv"
            final_df.to_csv(data_filepath, index=False)
            df_manifest.loc[prediction_time_str, model] = 1
            df_manifest.to_csv(OPENMETEO_MODEL_MANIFEST)
            db_con.logging({"script": model, "log_type": "success", "message": f"Openmeteo data fetch script completed for {model}, prediction time: {prediction_time_str}"})
        
        except Exception as e:
            e = traceback.format_exc()
            print(e)
            db_con.logging({"script": model, "log_type": "error", "message": f"Openmeteo data fetch script failed for {model}: {e}"})
            continue

db_con.logging({"script": SCRIPT_NAME, "log_type": "info", "message": f"Openmeteo data fetch script completed"})
