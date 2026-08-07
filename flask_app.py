from flask import Flask, request, jsonify
from sqlalchemy import text
from functools import wraps
import pandas as pd
from pangres import upsert
import yaml
import os
import traceback
from datetime import datetime, timezone
from urllib.parse import quote as urlquote
from sqlalchemy import create_engine
import psycopg2
import json
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address

'''
curl  -X GET "http://127.0.0.1:5000/pwr_fct/pull?plant_name=Loc_4110&fct_src=inhouse&model_name=intraday_wind&start_time=2026-03-01T00:00:00&end_time=2026-04-30T23:59:59" -H "x-api-key: abcd1234"

curl -X GET "http://127.0.0.1:5000/meas/pull?plant_name=Loc_4110&start_time=2026-03-01T00:00:00&end_time=2026-04-30T23:59:59" -H "x-api-key: abcd1234"

curl -X GET "http://127.0.0.1:5000/nwp/pull?plant_name=Loc_4110&model_name=ecmwf_ifs&start_time=2026-03-01T00:00:00&end_time=2026-04-30T23:59:59" -H "x-api-key: abcd1234"

curl -X GET "http://127.0.0.1:5000/static_table/pull" -H "x-api-key: abcd1234"
'''

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
    
    def get_weather_data(self, plant, model, start_date, end_date):
        ist = self.df_static[self.df_static["plant_name"] == plant].iloc[0]
        df_weather = pd.read_sql(f"select * from re_insight.weather_table \
            where plant_id = {ist['plant_id']} \
            and model_name = '{model}' \
            and forecast_time between '{start_date}' and '{end_date}'", con=self.conn)
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
    
    def get_meas_data(self, plant, start_date, end_date):
        ist = self.df_static[self.df_static["plant_name"] == plant].iloc[0]
        df_weather = pd.read_sql(f"select * from re_insight.meas_table \
            where plant_id = {ist['plant_id']} \
            and record_time between '{start_date}' and '{end_date}'", con=self.conn)
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
            log_dict["logging_time"] = datetime.now(timezone.utc)
        df_log = pd.DataFrame([log_dict])
        self.push_log_data(df_log)


CONFIG_PATH = os.getenv("WEATHER_CONFIG", "reinsight_config.yml")
with open(CONFIG_PATH, "r") as f:
    config = yaml.safe_load(f)

FLASK_API_KEY = config["flask_api_key"]

app = Flask(__name__)

limiter = Limiter(
    get_remote_address,
    app=app,
    default_limits=["2000 per day", "50 per hour"]
)
def require_api_key(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        # Check header first; if missing, check URL query parameters (?api_key=...)
        api_key = request.headers.get('X-API-Key') or request.args.get('api_key')
        
        if api_key != FLASK_API_KEY:
            return jsonify({"error": "Unauthorized"}), 401
            
        return f(*args, **kwargs)
    return decorated

DB_PATH = config["db_path"]

db_cred = config["db_cred"]

#### ADDING THIS TO MAKE FLASK PICK CREDENTIAL FROM ENV VARIABLE
db_host = os.environ.get('DB_HOST', db_cred['user_ip'])
db_user = os.environ.get('DB_USER', db_cred['user_name'])
db_pass = os.environ.get('DB_PASSWORD', db_cred['user_passwd'])
db_name = os.environ.get('DB_NAME', db_cred['db_name'])
db_port = os.environ.get('DB_PORT', db_cred['user_port'])

encoded_pass = urlquote(db_pass)
engine = create_engine(
    f"postgresql://{db_user}:{encoded_pass}@{db_host}:{db_port}/{db_name}"
)
db_columns = config["db_columns"]

db_con = DBcon(con=engine, db_schema=db_columns)

def log_error(endpoint, error_message, traceback_str=None):
    """
    Helper function to log errors into the logging_table.
    Assumes logging_table exists with columns: timestamp, endpoint, error_message, traceback
    """
    try:
        with engine.connect() as conn:
            run_time = datetime.now()
            conn.execute(
                "INSERT INTO re_insight.logging_table (run_time, endpoint, error_message, traceback) VALUES (?, ?, ?, ?)",
                [run_time, endpoint, error_message, traceback_str],
            )
    except Exception as e:
        # If logging itself fails, print to console as fallback
        print(f"Failed to log error to DB: {e}")


@app.route("/weather/push", methods=["POST"])
@require_api_key
def push_weather_data():
    """
    Push a single or multiple weather records into weather_table.
    Expects JSON: {"data": [{...}, {...}]}
    """
    data = request.json.get("data", [])
    if not data:
        return jsonify({"error": "No data provided"}), 400

    try:
        with engine.connect() as conn:
            # Using a DataFrame for efficient bulk insertion into DuckDB
            df = pd.DataFrame(data)

            # Register the DataFrame as a virtual table and insert
            columns = ", ".join([f'"{col}"' for col in df.columns])
            
            # We append ON CONFLICT DO NOTHING to automatically skip rows that violate the unique key constraint
            # and insert the new ones successfully instead of failing the entire operation.
            
            # Get count before insert
            initial_count = conn.execute("SELECT COUNT(*) FROM re_insight.weather_table").fetchone()[0]
            
            conn.execute(f"INSERT INTO re_insight.weather_table ({columns}) SELECT * FROM df ON CONFLICT DO NOTHING")
            
        # Get count after insert
        final_count = conn.execute("SELECT COUNT(*) FROM re_insight.weather_table").fetchone()[0]
        
        inserted_count = final_count - initial_count
        ignored_count = len(df) - inserted_count

        return jsonify({
            "message": f"Successfully processed {len(df)} records",
            "inserted": inserted_count,
            "ignored": ignored_count
        }), 201

    except psycopg2.IntegrityError as e:
        error_msg = str(e)
        log_error("/weather/push", f"ConstraintException: {error_msg}")
        return (
            jsonify(
                {
                    "error": "Unique constraint violation. Check if data already exists.",
                    "details": error_msg,
                }
            ),
            409,
        )
    except Exception as e:
        error_msg = str(e)
        tb_str = traceback.format_exc()
        log_error("/weather/push", error_msg, tb_str)
        return jsonify({"error": error_msg}), 500

@app.route("/nwp/pull", methods=["GET"])
@require_api_key
def pull_weather_data():
    """
    Pull weather data for a specific plant by its name.
    """
    plant_name = request.args.get("plant_name")
    model_name = request.args.get("model_name")
    start_time = request.args.get("start_time")
    end_time = request.args.get("end_time")

    try:
        df = db_con.get_weather_data(plant=plant_name, model=model_name,
                                        start_date=start_time, 
                                        end_date=end_time)
        result = df.to_dict(orient="records")

        return jsonify(result), 200

    except Exception as e:
        error_msg = str(e)
        tb_str = traceback.format_exc()
        log_error(f"/weather/pull/{plant_name}", error_msg, tb_str)
        return jsonify({"error": error_msg}), 500@app.route("/static_table/pull", methods=["GET"])

@app.route("/static_table/pull", methods=["GET"])
@require_api_key
def get_all_static_table():
    """
    Fetch all data from the static_table in the DuckDB database.
    """
    try:
        query = "SELECT * FROM re_insight.static_table"
        df = pd.read_sql(query, engine)
        
        # safely parses numpy arrays to native lists for proper jsonify serialization
        result = json.loads(df.to_json(orient="records"))
        return jsonify(result), 200
    except Exception as e:
        error_msg = str(e)
        tb_str = traceback.format_exc()
        log_error("/static_table/pull", error_msg, tb_str)
        return jsonify({"error": error_msg}), 500


@app.route("/static_table/push", methods=["POST"])
@require_api_key
def push_static_data():
    """
    Push a single or multiple static plant records into static_table.
    Expects JSON: {"data": [{...}, {...}]}
    """
    data = request.json.get("data", [])
    if not data:
        return jsonify({"error": "No data provided"}), 400

    try:
        # Using a DataFrame for efficient bulk insertion into DuckDB
        df = pd.DataFrame(data)

        # Drop 'id' if passing it, to rely on database auto-increment
        if 'id' in df.columns:
            df = df.drop(columns=['id'])

        # Register the DataFrame as a virtual table and insert or update
        columns = ", ".join([f'"{col}"' for col in df.columns])
        
        # Build the DO UPDATE SET clause to update all columns except plant_name
        update_cols = [col for col in df.columns if col.lower() != 'plant_name']
        set_clause = ", ".join([f'"{col}" = EXCLUDED."{col}"' for col in update_cols])
        
        upsert_query = f"""
            INSERT INTO re_insight.static_table ({columns}) 
            SELECT * FROM df
            ON CONFLICT (plant_name) 
            DO UPDATE SET {set_clause};
        """
        with engine.connect() as conn:
            conn.execute(upsert_query)
        return jsonify({"message": f"Successfully upserted {len(df)} static records"}), 201

    except psycopg2.IntegrityError as e:
        error_msg = str(e)
        log_error("/static_table/push", f"ConstraintException: {error_msg}")
        return (
            jsonify(
                {
                    "error": "Unique constraint violation. Check if data already exists.",
                    "details": error_msg,
                }
            ),
            409,
        )
    except Exception as e:
        error_msg = str(e)
        tb_str = traceback.format_exc()
        log_error("/static_table/push", error_msg, tb_str)
        return jsonify({"error": error_msg}), 500



@app.route("/meas/push", methods=["POST"])
@require_api_key
def push_meas_data():
    """Upload measurement records to meas_table via the API."""
    data = request.json.get("data", [])
    if not data:
        return jsonify({"error": "No data provided"}), 400
    try:
        df = pd.DataFrame(data)
        # Ensure the DataFrame columns match the DB schema; let DuckDB handle column mapping
        with engine.connect() as conn:
            conn.execute("INSERT INTO re_insight.meas_table SELECT * FROM df")
        return jsonify({"message": f"Inserted {len(df)} records"}), 201
    except psycopg2.IntegrityError as e:
        error_msg = str(e)
        log_error("/meas/push", f"ConstraintException: {error_msg}")
        return jsonify({"error": "Constraint violation", "details": error_msg}), 409
    except Exception as e:
        error_msg = str(e)
        tb_str = traceback.format_exc()
        log_error("/meas/push", error_msg, tb_str)
        return jsonify({"error": error_msg}), 500


@app.route("/meas/pull", methods=["GET"])
@require_api_key
def pull_meas_data():
    """
    Fetch measurement records for a specific plant name.
    Optional query params: ?start_time=ISO8601&end_time=ISO8601
    http://127.0.0.1:5000/meas/pull?plant_name=Loc_4110&start_time=2026-03-01T00:00:00&end_time=2026-04-30T23:59:59
    """
    start_time = request.args.get("start_time")
    end_time = request.args.get("end_time")
    plant_name = request.args.get("plant_name")
    try:
        df = db_con.get_meas_data(plant=plant_name, \
            start_date=start_time, \
            end_date=end_time)
        result = df.to_dict(orient="records")
        return jsonify(result), 200
    except Exception as e:
        error_msg = str(e)
        tb_str = traceback.format_exc()
        log_error(f"/meas/pull/{plant_name}", error_msg, tb_str)
        return jsonify({"error": error_msg}), 500



@app.route("/pwr_fct/pull", methods=["GET"])
@require_api_key
def pull_pwr_fct_data():
    """
    Fetch power forecast records for a specific plant name.
    Optional query params: ?start_time=ISO8601&end_time=ISO8601
    http://127.0.0.1:5000/pwr_fct/pull?plant_name=Loc_4110&fct_src=inhouse&model_name=intraday_wind&start_time=2026-03-01T00:00:00&end_time=2026-04-30T23:59:59
    """
    start_time = request.args.get("start_time")
    end_time = request.args.get("end_time")
    plant_name = request.args.get("plant_name")
    fct_src = request.args.get("fct_src")
    model_name = request.args.get("model_name")
    try:
        df = db_con.get_fct_data(plant=plant_name, fct_src=fct_src, model_name=model_name, start_date=start_time, end_date=end_time)
        result = df.to_dict(orient="records")
        return jsonify(result), 200
    except Exception as e:
        error_msg = str(e)
        tb_str = traceback.format_exc()
        log_error(f"/pwr_fct/pull/{plant_name}", error_msg, tb_str)
        return jsonify({"error": error_msg}), 500

if __name__ == "__main__":
    app.run()