from flask import Flask, request, jsonify
from functools import wraps
import duckdb
import pandas as pd
import yaml
import os
import traceback
from datetime import datetime
from urllib.parse import quote as urlquote
from sqlalchemy import create_engine, text
import psycopg2
import json
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address

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

engine = create_engine(
    f"postgresql://{db_cred['user_name']}:%s@{db_cred['user_ip']}:{db_cred['user_port']}/{db_cred['db_name']}"
    % urlquote(db_cred["user_passwd"])
)

def log_error(endpoint, error_message, traceback_str=None):
    """
    Helper function to log errors into the logging_table.
    Assumes logging_table exists with columns: timestamp, endpoint, error_message, traceback
    """
    try:
        with engine.connect() as conn:
            run_time = datetime.now()
            conn.execute(
                "INSERT INTO re_forecasting.logging_table (run_time, endpoint, error_message, traceback) VALUES (?, ?, ?, ?)",
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
            initial_count = conn.execute("SELECT COUNT(*) FROM re_forecasting.weather_table").fetchone()[0]
            
            conn.execute(f"INSERT INTO re_forecasting.weather_table ({columns}) SELECT * FROM df ON CONFLICT DO NOTHING")
            
        # Get count after insert
        final_count = conn.execute("SELECT COUNT(*) FROM re_forecasting.weather_table").fetchone()[0]
        
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


@app.route("/weather/pull/<string:plant_name>", methods=["GET"])
@require_api_key
def pull_weather_data(plant_name):
    """
    Pull weather data for a specific plant by its name.
    Optional query param: ?model_name=ECMWF
    http://127.0.0.1:5000/weather/pull/vayu?model_name=ncm_d2&start_date=2026-03-27T00:00:00&end_date=2026-03-29T15:00:00
    """
    model_name = request.args.get("model_name")
    start_date = request.args.get("start_date")
    end_date = request.args.get("end_date")

    try:
        
        # We perform a JOIN to fetch weather rows where the corresponding static table plant_name matches
        query = """
            SELECT w.* 
            FROM re_forecasting.weather_table w
            JOIN re_forecasting.static_table s ON w.plant_id = s.id
            WHERE s.plant_name = ?
        """
        params = [plant_name]

        if model_name:
            query += " AND w.model_name = ?"
            params.append(model_name)
            
        if start_date:
            query += " AND w.forecast_time >= ?"
            params.append(start_date)
            
        if end_date:
            query += " AND w.forecast_time <= ?"
            params.append(end_date)

        # Fetch as a list of dictionaries
        df = pd.read_sql(query, engine, params=params)
        result = df.to_dict(orient="records")

        return jsonify(result), 200
    except Exception as e:
        error_msg = str(e)
        tb_str = traceback.format_exc()
        log_error(f"/weather/pull/{plant_name}", error_msg, tb_str)
        return jsonify({"error": error_msg}), 500

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
            INSERT INTO re_forecasting.static_table ({columns}) 
            SELECT * FROM df
            ON CONFLICT (plant_name) 
            DO UPDATE SET {set_clause};
        """
        with engine.connect() as conn:
            conn.execute(upsert_query)
        return jsonify({"message": f"Successfully upserted {len(df)} static records"}), 201

    except duckdb.ConstraintException as e:
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
            conn.execute("INSERT INTO re_forecasting.meas_table SELECT * FROM df")
        return jsonify({"message": f"Inserted {len(df)} records"}), 201
    except duckdb.ConstraintException as e:
        error_msg = str(e)
        log_error("/meas/push", f"ConstraintException: {error_msg}")
        return jsonify({"error": "Constraint violation", "details": error_msg}), 409
    except Exception as e:
        error_msg = str(e)
        tb_str = traceback.format_exc()
        log_error("/meas/push", error_msg, tb_str)
        return jsonify({"error": error_msg}), 500


@app.route("/meas/pull/<string:plant_name>", methods=["GET"])
@require_api_key
def pull_meas_data(plant_name):
    """
    Fetch measurement records for a specific plant name.
    Optional query params: ?start_time=ISO8601&end_time=ISO8601
    http://127.0.0.1:5000/meas/pull/vayu?start_time=2026-03-01T00:00:00&end_time=2026-04-30T23:59:59
    """
    start_time = request.args.get("start_time")
    end_time = request.args.get("end_time")
    try:
        query = """
            SELECT m.* FROM re_forecasting.meas_table m
            JOIN re_forecasting.static_table s ON m.plant_id = s.id
            WHERE s.plant_name = ?
        """
        params = [plant_name]
        if start_time:
            query += " AND m.record_time >= ?"
            params.append(start_time)
        if end_time:
            query += " AND m.record_time <= ?"
            params.append(end_time)
        df = pd.read_sql(query, engine, params=params)
        result = df.to_dict(orient="records")
        return jsonify(result), 200
    except Exception as e:
        error_msg = str(e)
        tb_str = traceback.format_exc()
        log_error(f"/meas/pull/{plant_name}", error_msg, tb_str)
        return jsonify({"error": error_msg}), 500

if __name__ == "__main__":
    app.run()