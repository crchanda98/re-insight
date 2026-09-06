import os
import pytz
import yaml
import numpy as np
import pandas as pd
import xgboost as xgb
import utils
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from urllib.parse import quote as urlquote

# -------------------------------------------------------------------
# 1. CONFIGURATION & SETUP
# -------------------------------------------------------------------
CONFIG_PATH = os.getenv("WEATHER_CONFIG", "reinsight_config.yml")
with open(CONFIG_PATH, "r") as f:
    CONFIG = yaml.safe_load(f)

db_cred = CONFIG["db_cred"]
# db_cred = CONFIG
# engine = create_engine(
#     f"postgresql://{db_cred['reinsight_db_config']['user_name']}:%s@{db_cred['reinsight_db_config']['user_ip']}:{db_cred['reinsight_db_config']['user_port']}/{db_cred['reinsight_db_config']['dbname']}"
#     % urlquote(db_cred["reinsight_db_config"]["user_passwd"])
# )

engine = create_engine(
    f"postgresql://{db_cred['user_name']}:%s@{db_cred['user_ip']}:{db_cred['user_port']}/{db_cred['db_name']}"
    % urlquote(db_cred["user_passwd"])
)

db_columns = CONFIG["db_columns"]
fct_table_column = db_columns["forecast_table"]["columns"]
fct_table_column_un = db_columns["forecast_table"]["unique_constraint"]

db_con = utils.DBcon(con = engine, db_schema=db_columns)

df_static = db_con.get_static_data()
df_static = df_static[df_static["plant_id"].isin([1, 2, 3])]
df_static = df_static.set_index("plant_id")
# db_con.logging({"script": SCRIPT_NAME, "log_type": "info", "message": f"Intraday wind script started"})

PLANT_CAPACITIES_KW = {
    1: 48000,  # 48.0 MW
    # 2: 25600,  # 25.6 MW
    # 3: 10400,  # 10.4 MW
}


ist_tz = pytz.timezone("Asia/Kolkata")
now_ist = datetime.now(ist_tz)
today = now_ist.date()
tomorrow = today + timedelta(days=1)

fetch_start_date = today - timedelta(days=35)
fetch_start_str = fetch_start_date.strftime("%Y-%m-%d 00:00:00")
fetch_end_str = tomorrow.strftime("%Y-%m-%d 23:59:59")


# -------------------------------------------------------------------
# 2. DATA FETCHING
# -------------------------------------------------------------------
def fetch_and_merge_training_data(engine, plant_id, start_date, end_date):
    # Fetch Actual Measurements
    meas_query = text("""
        SELECT record_time, active_power
        FROM re_insight.meas_table
        WHERE plant_id = :plant_id
          AND record_time >= :start_date 
          AND record_time <= :end_date
    """)
    df_meas = pd.read_sql(
        meas_query,
        engine,
        params={"plant_id": plant_id, "start_date": start_date, "end_date": end_date},
    )
    df_meas["record_time"] = pd.to_datetime(df_meas["record_time"])

    # Helper for weather
    def fetch_weather_model(model_name, prefix):
        weather_query = text("""
            SELECT DISTINCT ON (forecast_time) 
                forecast_time, wind_speed, wind_direction
            FROM re_insight.weather_table
            WHERE plant_id = :plant_id
              AND height = 80.0
              AND model_name = :model_name
              AND forecast_time >= :start_date 
              AND forecast_time <= :end_date
            ORDER BY forecast_time ASC, prediction_time DESC
        """)
        df = pd.read_sql(
            weather_query,
            engine,
            params={
                "plant_id": plant_id,
                "model_name": model_name,
                "start_date": start_date,
                "end_date": end_date,
            },
        )
        df["forecast_time"] = pd.to_datetime(df["forecast_time"])
        df = df.set_index("forecast_time").resample("15min").ffill().reset_index()
        return df.rename(
            columns={
                "wind_speed": f"wind_speed_{prefix}",
                "wind_direction": f"wind_dir_{prefix}",
            }
        )

    df_ecmwf = fetch_weather_model("ecmwf_ifs", "ecmwf")
    df_ncm = fetch_weather_model("ncm_d2", "ncm")

    df_weather_combined = pd.merge(df_ecmwf, df_ncm, on="forecast_time", how="outer")
    df_merged = pd.merge(
        df_weather_combined,
        df_meas,
        left_on="forecast_time",
        right_on="record_time",
        how="outer",
    ).drop(columns=["record_time"])

    # Convert to IST
    if df_merged["forecast_time"].dt.tz is None:
        df_merged["forecast_time"] = df_merged["forecast_time"].dt.tz_localize("UTC")
    df_merged["forecast_time"] = df_merged["forecast_time"].dt.tz_convert(ist_tz)
    df_merged = df_merged.sort_values("forecast_time").reset_index(drop=True)

    # Reindex to ensure no missing 15-min blocks
    full_time_range = pd.date_range(
        start=df_merged["forecast_time"].min(),
        end=df_merged["forecast_time"].max(),
        freq="15min",
    )
    df_merged = (
        df_merged.set_index("forecast_time")
        .reindex(full_time_range)
        .rename_axis("forecast_time")
        .reset_index()
    )

    weather_cols = [col for col in df_merged.columns if "wind" in col]
    df_merged[weather_cols] = df_merged[weather_cols].ffill()

    # Note: For Tomorrow, active_power will be NaN (which is correct, it hasn't happened yet)
    df_merged["active_power"] = df_merged["active_power"].interpolate(method="linear")
    return df_merged


# -------------------------------------------------------------------
# 3. FEATURE ENGINEERING
# -------------------------------------------------------------------
def create_base_features(df):
    df = df.copy()
    for prefix in ["ecmwf", "ncm"]:
        wd_rad = np.deg2rad(df[f"wind_dir_{prefix}"])
        df[f"wind_dir_sin_{prefix}"] = np.sin(wd_rad)
        df[f"wind_dir_cos_{prefix}"] = np.cos(wd_rad)
        df = df.drop(columns=[f"wind_dir_{prefix}"])

    df["wind_speed_mean"] = (df["wind_speed_ecmwf"] + df["wind_speed_ncm"]) / 2.0
    df["wind_speed_delta"] = df["wind_speed_ecmwf"] - df["wind_speed_ncm"]
    df["wind_dir_alignment"] = (df["wind_dir_cos_ecmwf"] * df["wind_dir_cos_ncm"]) + (
        df["wind_dir_sin_ecmwf"] * df["wind_dir_sin_ncm"]
    )

    df["hour"] = df["forecast_time"].dt.hour
    df["minute"] = df["forecast_time"].dt.minute
    df["time_of_day"] = df["hour"] + df["minute"] / 60.0
    df["month"] = df["forecast_time"].dt.month
    df["day_of_year"] = df["forecast_time"].dt.dayofyear
    df = df.drop(columns=["hour", "minute"])
    return df


def create_day_ahead_lags(df):
    df = df.copy()
    df_power = df[["forecast_time", "active_power"]].copy()
    df_power["forecast_time"] = df_power["forecast_time"] + pd.Timedelta(hours=48)
    df_power = df_power.rename(columns={"active_power": "power_lag_48h"})
    df = pd.merge(df, df_power, on="forecast_time", how="left")

    lags = [(60, "1h"), (75, "1h_15min"), (90, "1h_30min")]
    for prefix in ["ecmwf", "ncm"]:
        for minutes, lag_name in lags:
            df_w = df[["forecast_time", f"wind_speed_{prefix}"]].copy()
            df_w["forecast_time"] = df_w["forecast_time"] + pd.Timedelta(
                minutes=minutes
            )
            df_w = df_w.rename(
                columns={f"wind_speed_{prefix}": f"wind_speed_{prefix}_lag_{lag_name}"}
            )
            df = pd.merge(df, df_w, on="forecast_time", how="left")

    df = df.dropna().reset_index(drop=True)
    if "prediction_time" in df.columns:
        df = df.drop(columns=["prediction_time"])
    return df


# -------------------------------------------------------------------
# 4. TRAINING & PREDICTION
# -------------------------------------------------------------------
def generate_tomorrow_forecast(df, train_window_days=30):
    features = [
        "wind_speed_ecmwf",
        "wind_dir_sin_ecmwf",
        "wind_dir_cos_ecmwf",
        "wind_speed_ecmwf_lag_1h",
        "wind_speed_ecmwf_lag_1h_15min",
        "wind_speed_ecmwf_lag_1h_30min",
        "wind_speed_ncm",
        "wind_dir_sin_ncm",
        "wind_dir_cos_ncm",
        "wind_speed_ncm_lag_1h",
        "wind_speed_ncm_lag_1h_15min",
        "wind_speed_ncm_lag_1h_30min",
        "wind_speed_mean",
        "wind_speed_delta",
        "wind_dir_alignment",
    ]
    target = "active_power"

    df = df.copy()
    df["date"] = df["forecast_time"].dt.date

    yesterday = today - timedelta(days=1)
    train_start_date = yesterday - timedelta(days=train_window_days)

    train_df = df[(df["date"] >= train_start_date) & (df["date"] <= yesterday)].copy()

    test_df = df[df["date"] == tomorrow].copy()

    train_df = train_df.dropna(subset=[target])

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
    model.fit(train_df[features], train_df[target])

    unique_wind_speeds = test_df["wind_speed_ecmwf"].nunique()
    if unique_wind_speeds <= 1:
        raise ValueError(
            f"🚨 WARNING: Weather data for {tomorrow} is missing or flatlined! The database only has {unique_wind_speeds} unique wind speed value for the whole day. Halting forecast."
        )

    predictions = model.predict(test_df[features])
    test_df["predicted_power"] = predictions.clip(min=0)

    test_df["actual_power"] = np.nan

    return test_df


# -------------------------------------------------------------------
# 5. EXECUTION & CSV GENERATION
# -------------------------------------------------------------------
if __name__ == "__main__":
    print(f"Generating D+1 Forecast for {tomorrow}...")

    all_plants_forecast = []
    date_now = utils.get_last_15_min_slot()
    fct_start_time = date_now + timedelta(days=1)

    for current_plant_id, current_avc in PLANT_CAPACITIES_KW.items():
        print(f"Processing Plant ID: {current_plant_id}")

        # 1. Fetch
        df_raw = fetch_and_merge_training_data(
            engine, current_plant_id, fetch_start_str, fetch_end_str
        )

        # 2. Features & Lags
        df_features = create_base_features(df_raw)
        df_model_ready = create_day_ahead_lags(df_features)

        # 3. Predict Tomorrow
        df_tomorrow_forecast = generate_tomorrow_forecast(
            df_model_ready, train_window_days=30
        )

        # 4. Format Output
        df_tomorrow_forecast["plant_id"] = current_plant_id

        cols_to_export = [
            "plant_id",
            "forecast_time",
            "actual_power",
            "predicted_power",
            "wind_speed_mean",
            "wind_speed_delta",
            "wind_dir_alignment",
        ]

        all_plants_forecast.append(df_tomorrow_forecast[cols_to_export])

    df_fct = pd.concat(all_plants_forecast, ignore_index=True)

    df_fct["prediction_time"] = date_now
    df_fct["prediction_time"] = df_fct["prediction_time"].dt.tz_localize("Asia/Kolkata")
    df_fct["forecast_source"] = "inhouse"
    df_fct["model_name"] = "da_rf"
    df_fct = df_fct.rename({"predicted_power": "active_power"}, axis = 1)
    fct_filename = f"../data_lake/re_insights/rel_time_fct/dayahead_wind_rf_{fct_start_time.strftime('%Y%m%d')}.csv"
    df_fct.to_csv(fct_filename, index=False)

    df_all = pd.DataFrame(columns=fct_table_column)
    df_fct = pd.concat([df_all, df_fct], ignore_index=True)
    df_fct = df_fct.dropna(how = "all")
    df_fct = df_fct[fct_table_column]
    df_fct = df_fct.set_index(fct_table_column_un)
    db_con.push_fct_data(df_fct)
    print(f"✅ Forecast successfully generated and saved to {fct_filename}")
