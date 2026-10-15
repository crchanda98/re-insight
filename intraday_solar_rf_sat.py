import pandas as pd
import yaml
import os
import traceback
from pvlib.location import Location
import utils
from datetime import datetime as dt, timedelta
from urllib.parse import quote as urlquote
from sqlalchemy import create_engine
from ftplib import FTP
from sklearn.ensemble import RandomForestRegressor
import warnings
from sklearn.linear_model import LinearRegression

warnings.filterwarnings("ignore")

CONFIG_PATH = os.getenv("WEATHER_CONFIG", "reinsight_config.yml")
with open(CONFIG_PATH, "r") as f:
    config = yaml.safe_load(f)

SCRIPT_NAME = os.path.basename(__file__)

FTP_HOST = config["fct_ftp_cred"]["host"]
FTP_USER = config["fct_ftp_cred"]["user"]
FTP_PASS = config["fct_ftp_cred"]["password"]
MODEL_NAME = "intraday_solar_rf_sat"
LOGGING = True

date_now = utils.get_last_15_min_slot()
print(f"Code started at {dt.now()}")
date_now = date_now - timedelta(hours=5, minutes=30)
date_now_tz = pd.Timestamp.now(tz="UTC")


def push_fct_to_ftp(filename):
    with FTP(FTP_HOST) as ftp:
        ftp.login(user=FTP_USER, passwd=FTP_PASS)
        ftp.cwd("/home/ftpuser/ftp/upload")
        with open(filename, "rb") as file:
            ftp.storbinary(f"STOR {filename}", file)


def proc_nwp_ghi(df_d, height):
    df_n = df_d[df_d["height"] == height]
    df_n = df_n.sort_values(
        by=["forecast_time", "prediction_time"], ascending=[True, False]
    )
    df_n = df_n.drop_duplicates(subset="forecast_time", keep="first")
    df_n = df_n[["forecast_time", "ghi", "low_cloud", "medium_cloud"]]
    df_n = df_n.dropna(axis=1, how="all")
    df_n = df_n.set_index("forecast_time")
    df_n = df_n.resample("15min").interpolate(method="time")
    return df_n


def get_clear_sky(dfi, index_col=None):
    if index_col is not None:
        dfi = dfi.set_index(index_col)
    dfi["day_hour"] = dfi.index.hour * 100 + dfi.index.minute
    idf_cs = dfi.groupby("day_hour")[["active_power"]].max()
    idf_cs = idf_cs.reset_index()
    idf_cs = idf_cs.rename({"active_power": "active_power_cs"}, axis=1)
    idf_cs = idf_cs[["day_hour", "active_power_cs"]]
    return idf_cs


db_cred = config["db_cred"]

engine = create_engine(
    f"postgresql://{db_cred['user_name']}:%s@{db_cred['user_ip']}:{db_cred['user_port']}/{db_cred['db_name']}"
    % urlquote(db_cred["user_passwd"])
)
db_columns = config["db_columns"]
fct_table_column = db_columns["forecast_table"]["columns"]
fct_table_column_un = db_columns["forecast_table"]["unique_constraint"]

db_con = utils.DBcon(con=engine, db_schema=db_columns)
if LOGGING:
    db_con.logging(
        {
            "script": SCRIPT_NAME,
            "log_type": "info",
            "message": f"Intraday wind script started",
        }
    )

df_static = db_con.get_static_data()
df_static = df_static[df_static["plant_id"].isin([20])]

fct_start_time = date_now + timedelta(hours=1, minutes=30)
fct_end_time = fct_start_time + timedelta(hours=6)

train_end_time = date_now
train_start_time = train_end_time - timedelta(days=7)

fct_dir = f"../data_lake/re_insights/model_fct/{MODEL_NAME}"
os.makedirs(fct_dir, exist_ok=True)
EXOGEN_VAR = ["ghi"]

for _, idf in df_static.iterrows():
    try:
        print(f"Running ID for {idf['plant_name']}")
        if LOGGING:
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

        latitude = idf["latitude"]
        longitude = idf["longitude"]
        tz = "UTC"
        altitude = 70

        site = Location(latitude, longitude, tz=tz, altitude=altitude)

        # 3. Create a localized pandas DatetimeIndex for your time window
        times = pd.date_range(
            start=fct_start_time, end=fct_end_time, freq="15min", tz=site.tz
        )

        # 4. Calculate clear sky irradiance
        # By default, this uses the 'ineichen' model with integrated climatology tables
        clearsky_df = site.get_clearsky(times)
        clearsky_df.index.name = "record_time"
        clearsky_df = clearsky_df.reset_index()
        clearsky_df = clearsky_df.rename(columns={"ghi": "ghi_cs"})

        ### Fetching training data
        meas_data = db_con.get_meas_data(
            plant=plant_name,
            start_date=train_start_time.strftime("%Y-%m-%dT%H:%M:%S"),
            end_date=train_end_time.strftime("%Y-%m-%dT%H:%M:%S"),
        )
        latest_ftp_time = meas_data["record_time"].max()
        meas_data = meas_data[["record_time", "active_power"]]
        meas_data.loc[meas_data["active_power"] < 0, "active_power"] = 0
        meas_data.loc[meas_data["active_power"] > avc, "active_power"] = float("nan")

        if LOGGING:
            db_con.logging(
                {
                    "script": SCRIPT_NAME,
                    "log_type": "info",
                    "message": f"Meas data for {plant_name} with latest record time {latest_ftp_time}",
                }
            )

        ### Fetching nwp model training data
        for i, imodel in enumerate(
            [
                "ecmwf_ifs",
                "ecmwf_ifs025",
                "gfs_global",
                "icon_global",
                "ukmo_global_deterministic_10km",
                "meteofrance_arpege_world",
            ]
        ):
            nwp_data = db_con.get_weather_data(
                plant=plant_name,
                model=imodel,
                start_date=train_start_time.strftime("%Y-%m-%dT%H:%M:%S"),
                end_date=fct_end_time.strftime("%Y-%m-%dT%H:%M:%S"),
            )
            df_nwp_i = proc_nwp_ghi(nwp_data, height=10)
            df_nwp_i = df_nwp_i.add_suffix("_" + imodel)
            df_nwp_i = df_nwp_i.reset_index()
            if i == 0:
                df_nwp = df_nwp_i
            else:
                df_nwp = pd.merge(df_nwp, df_nwp_i, on="forecast_time", how="left")

        nwp_data = db_con.get_weather_data(
            plant=plant_name,
            model="ncum_r",
            start_date=train_start_time.strftime("%Y-%m-%dT%H:%M:%S"),
            end_date=fct_end_time.strftime("%Y-%m-%dT%H:%M:%S"),
        )
        df_nwp_i = proc_nwp_ghi(nwp_data, height=0)
        df_nwp_i = df_nwp_i.add_suffix("_" + "ncum_r")
        df_nwp_i = df_nwp_i.reset_index()
        df_nwp = pd.merge(df_nwp, df_nwp_i, on="forecast_time", how="left")

        nwp_data = db_con.get_weather_data(
            plant=plant_name,
            model="ncum_g",
            start_date=train_start_time.strftime("%Y-%m-%dT%H:%M:%S"),
            end_date=fct_end_time.strftime("%Y-%m-%dT%H:%M:%S"),
        )
        df_nwp_i = proc_nwp_ghi(nwp_data, height=0)
        df_nwp_i = df_nwp_i.add_suffix("_" + "ncum_g")
        df_nwp_i = df_nwp_i.reset_index()
        df_nwp = pd.merge(df_nwp, df_nwp_i, on="forecast_time", how="left")
        df_nwp = df_nwp.rename({"forecast_time": "record_time"}, axis=1)
        EXOGEN_VAR_NO_SAT = [col for col in df_nwp.columns if "ghi" in col]

        cmv_data = db_con.get_weather_data(
            plant=plant_name,
            model="ncm_sat",
            start_date=train_start_time.strftime("%Y-%m-%dT%H:%M:%S"),
            end_date=fct_end_time.strftime("%Y-%m-%dT%H:%M:%S"),
        )

        cmv_data["block"] = (
            (cmv_data["forecast_time"] - cmv_data["prediction_time"]).dt.total_seconds()
            / 60
            / 15
        )
        cmv_latest_time = cmv_data["prediction_time"].max()
        cmv_forecast_time = cmv_data["forecast_time"].max()
        cmv_lag = date_now - cmv_latest_time.tz_convert(None).to_pydatetime()
        cmv_fct_lag = cmv_forecast_time.tz_convert(None).to_pydatetime() - date_now
        cmv_lag = cmv_lag.total_seconds() / 60
        cmv_fct_lag = cmv_fct_lag.total_seconds() / 60
        total_block_diff = cmv_lag / 15 + 8
        if LOGGING:
            db_con.logging(
                {
                    "script": SCRIPT_NAME,
                    "log_type": "info",
                    "message": f"CMV lag is {cmv_lag} minutes",
                }
            )

        # cmv_data = cmv_data.set_index("forecast_time")
        df_sat_monitor = cmv_data[cmv_data["block"] == 0]  # Keep only blocks 1 to 8
        df_sat_monitor = proc_nwp_ghi(df_sat_monitor, height=0)
        df_sat_monitor = df_sat_monitor.add_suffix("_" + "ncm_sat_monitor")
        df_sat_monitor = df_sat_monitor.reset_index()
        df_sat_monitor = df_sat_monitor.rename({"forecast_time": "record_time"}, axis=1)

        df_sat_nwcst = cmv_data[
            cmv_data["block"] == total_block_diff
        ]  # Keep only blocks 1 to 8
        df_fct_sat = pd.DataFrame()

        df_sat_nwcst_all = cmv_data[cmv_data["forecast_time"] > date_now_tz]

        df_sat_nwcst_all = pd.concat([df_sat_nwcst_all, df_fct_sat])
        df_sat_nwcst_all = df_sat_nwcst_all.sort_values(by=["forecast_time", "block"])
        df_sat_nwcst_all = df_sat_nwcst_all.drop_duplicates(
            subset="forecast_time", keep="first"
        )
        df_sat_nwcst_all = df_sat_nwcst_all.rename(
            {"forecast_time": "record_time"}, axis=1
        )

        if len(df_sat_nwcst) == 0:
            print(f"Laltest CMV not available. Lag is {cmv_lag}")

        df_model = pd.merge(meas_data, df_nwp, on="record_time", how="outer")
        meas_data_cs = get_clear_sky(
            df_model[["record_time", "active_power"]].dropna(), index_col="record_time"
        )

        if date_now.hour >= 2 and len(df_sat_nwcst) > 0:
            df_sat_nwcst = proc_nwp_ghi(df_sat_nwcst, height=0)
            df_sat_nwcst = df_sat_nwcst.add_suffix("_" + "ncm_sat_nwcst")
            df_sat_nwcst = df_sat_nwcst.reset_index()
            df_sat_nwcst = df_sat_nwcst.rename({"forecast_time": "record_time"}, axis=1)

            EXOGEN_VAR_SAT = [col for col in df_nwp.columns if "ghi" in col]

            df_model = pd.merge(meas_data, df_nwp, on="record_time", how="outer")

            df_model = df_model.set_index("record_time")

            df_model = df_model.asfreq("15min")

            df_train = df_model.loc[
                train_start_time.strftime(
                    "%Y-%m-%dT%H:%M:%S"
                ) : train_end_time.strftime("%Y-%m-%dT%H:%M:%S")
            ]
            df_train = df_train[df_train.index.hour >= 4]
            df_train = df_train.dropna()

            df_fct_sat_master = df_model.loc[
                fct_start_time.strftime(
                    "%Y-%m-%dT%H:%M:%S"
                ) : cmv_forecast_time.strftime("%Y-%m-%dT%H:%M:%S")
            ]

            df_fct_sat = df_fct_sat_master.dropna(subset=EXOGEN_VAR_SAT)

            if len(df_fct_sat) > 0:
                df_train = df_train.dropna(subset="active_power")
                model = LinearRegression()
                model.fit(df_train[EXOGEN_VAR_SAT], df_train["active_power"])
                pred = model.predict(df_fct_sat[EXOGEN_VAR_SAT])
                df_fct_sat["active_power"] = pred
                df_fct_sat = df_fct_sat.reset_index()
                df_fct_sat["model_name"] = "intraday_linreg_sat"
            else:
                print("No forecast data")

        # #### CREATE DEFAULT FORECAST WITHOUT SATELLITE
        df_train = df_model.loc[
            train_start_time.strftime("%Y-%m-%dT%H:%M:%S") : train_end_time.strftime(
                "%Y-%m-%dT%H:%M:%S"
            )
        ]
        df_fct_no_sat = df_model.loc[
            fct_start_time.strftime("%Y-%m-%dT%H:%M:%S") : fct_end_time.strftime(
                "%Y-%m-%dT%H:%M:%S"
            )
        ]

        df_fct_no_sat = df_fct_no_sat.dropna(subset=EXOGEN_VAR_NO_SAT)
        df_train = df_train.dropna()

        model = LinearRegression()
        model.fit(df_train[EXOGEN_VAR_NO_SAT], df_train["active_power"])
        pred = model.predict(df_fct_no_sat[EXOGEN_VAR_NO_SAT])
        df_fct_no_sat["active_power"] = pred
        df_fct_no_sat = df_fct_no_sat.reset_index()
        df_fct_no_sat["model_name"] = "intraday_linreg_no_sat"

        df_fct_comb = pd.concat([df_fct_sat, df_fct_no_sat], ignore_index=True)
        df_fct_comb = df_fct_comb.drop_duplicates(subset="record_time", keep="first")
        df_fct_comb["model_name"] = "intraday_sat_nwp_combo"

        df_fct = pd.concat(
            [
                df_fct_sat,
                df_fct_no_sat,
                df_fct_comb,
            ],
            ignore_index=True,
        )
        df_fct = df_fct.merge(
            clearsky_df[["record_time", "ghi_cs"]], on="record_time", how="left"
        )
        df_fct.loc[df_fct["ghi_cs"] == 0, "active_power"] = 0
        df_fct.to_csv(
            f"{fct_dir}/intraday_solar_raw_data_{MODEL_NAME}_{plant_name}_{date_now.strftime('%Y%m%d_%H%M')}.csv",
            index=False,
        )
        df_fct = df_fct.set_index("record_time")
        df_fct = df_fct.dropna(subset="active_power")
        df_fct["day_hour"] = df_fct.index.hour * 100 + df_fct.index.minute
        df_fct = df_fct.reset_index()
        df_fct = pd.merge(
            df_fct,
            meas_data_cs[["day_hour", "active_power_cs"]],
            on="day_hour",
            how="left",
        )

        df_fct["active_power"][df_fct["active_power_cs"] == 0] = 0
        df_fct["active_power"][df_fct["active_power"] < 0] = 0
        df_fct.loc[df_fct["active_power_cs"] <= 0, "active_power"] = 0
        df_fct.loc[
            df_fct["active_power"] >= df_fct["active_power_cs"], "active_power"
        ] = df_fct["active_power_cs"]
        df_fct = df_fct.reset_index()
        df_fct = df_fct.dropna(subset=["active_power"])
        df_fct["plant_name"] = plant_name
        df_fct["plant_id"] = df_static_plant["plant_id"]
        df_fct = df_fct[
            ["record_time", "active_power", "plant_name", "plant_id", "model_name"]
        ]
        df_fct = df_fct.rename({"record_time": "forecast_time"}, axis=1)
        df_fct = df_fct.round(2)
        df_fct["forecast_time"] = df_fct["forecast_time"].dt.tz_convert("Asia/Kolkata")
        df_fct["prediction_time"] = date_now
        df_fct["prediction_time"] = df_fct["prediction_time"].dt.tz_localize("UTC")
        df_fct["forecast_source"] = "inhouse"
        df_fct.to_csv(
            f"{fct_dir}/intraday_solar_{MODEL_NAME}_{plant_name}_{date_now.strftime('%Y%m%d_%H%M')}.csv",
            index=False,
        )

        df_all = pd.DataFrame(columns=fct_table_column)
        df_fct = pd.concat([df_all, df_fct], ignore_index=True)
        df_fct = df_fct.dropna(how="all")
        df_fct = df_fct[fct_table_column]
        df_fct = df_fct.set_index(fct_table_column_un)
        db_con.push_fct_data(df_fct)
        if LOGGING:
            db_con.logging(
                {
                    "script": SCRIPT_NAME,
                    "log_type": "success",
                    "message": f"FCT data generated for {plant_name}",
                }
            )
    except Exception as e:
        e = traceback.format_exc()
        print(f"An error occurred: {e}")
        if LOGGING:
            db_con.logging(
                {
                    "script": SCRIPT_NAME,
                    "log_type": "error",
                    "message": f"FCT data generation failed for {plant_name}: {e}",
                }
            )

if LOGGING:
    db_con.logging(
        {
            "script": SCRIPT_NAME,
            "log_type": "info",
            "message": f"Intraday solar CMV script completed",
        }
    )
print(f"Code ccompleted at {dt.now()}")
