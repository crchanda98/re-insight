from flask import Flask, request, jsonify
import duckdb
import pandas as pd
import yaml
import os
import traceback
from datetime import datetime

CONFIG_PATH = os.getenv("WEATHER_CONFIG", "reinsight_config.yml")
with open(CONFIG_PATH, "r") as f:
    config = yaml.safe_load(f)

app = Flask(__name__)
DB_PATH = config["db_path"]

def get_db_connection():
    # Connects to the DuckDB file
    return duckdb.connect(DB_PATH)


def log_error(endpoint, error_message, traceback_str=None):
    """
    Helper function to log errors into the logging_table.
    Assumes logging_table exists with columns: timestamp, endpoint, error_message, traceback
    """
    try:
        conn = get_db_connection()
        timestamp = datetime.now()
        conn.execute(
            "INSERT INTO re_forecasting.logging_table (timestamp, endpoint, error_message, traceback) VALUES (?, ?, ?, ?)",
            [timestamp, endpoint, error_message, traceback_str],
        )
        conn.close()
    except Exception as e:
        # If logging itself fails, print to console as fallback
        print(f"Failed to log error to DB: {e}")


@app.route("/weather/push", methods=["POST"])
def push_weather_data():
    """
    Push a single or multiple weather records into weather_table.
    Expects JSON: {"data": [{...}, {...}]}
    """
    data = request.json.get("data", [])
    if not data:
        return jsonify({"error": "No data provided"}), 400

    try:
        conn = get_db_connection()
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

        conn.close()
        return jsonify({
            "message": f"Successfully processed {len(df)} records",
            "inserted": inserted_count,
            "ignored": ignored_count
        }), 201

    except duckdb.ConstraintException as e:
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
def pull_weather_data(plant_name):
    """
    Pull weather data for a specific plant by its name.
    Optional query param: ?model_name=ECMWF
    """
    model_name = request.args.get("model_name")
    start_date = request.args.get("start_date")
    end_date = request.args.get("end_date")

    try:
        conn = get_db_connection()
        
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
            query += " AND w.prediction_time >= ?"
            params.append(start_date)
            
        if end_date:
            query += " AND w.prediction_time <= ?"
            params.append(end_date)

        # Fetch as a list of dictionaries
        result = conn.execute(query, params).fetchdf().to_dict(orient="records")
        conn.close()

        return jsonify(result), 200
    except Exception as e:
        error_msg = str(e)
        tb_str = traceback.format_exc()
        log_error(f"/weather/pull/{plant_id}", error_msg, tb_str)
        return jsonify({"error": error_msg}), 500

@app.route("/static_table/all", methods=["GET"])
def get_all_static_table():
    """
    Fetch all data from the static_table in the DuckDB database.
    """
    try:
        import json
        conn = get_db_connection()
        query = "SELECT * FROM re_forecasting.static_table"
        df = conn.execute(query).fetchdf()
        
        # safely parses numpy arrays to native lists for proper jsonify serialization
        result = json.loads(df.to_json(orient="records"))
        
        conn.close()
        return jsonify(result), 200
    except Exception as e:
        error_msg = str(e)
        tb_str = traceback.format_exc()
        log_error("/static_table/all", error_msg, tb_str)
        return jsonify({"error": error_msg}), 500


@app.route("/static_table/push", methods=["POST"])
def push_static_data():
    """
    Push a single or multiple static plant records into static_table.
    Expects JSON: {"data": [{...}, {...}]}
    """
    data = request.json.get("data", [])
    if not data:
        return jsonify({"error": "No data provided"}), 400

    try:
        conn = get_db_connection()
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
        conn.execute(upsert_query)
        conn.close()
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


if __name__ == "__main__":
    app.run(debug=True, port=5000)
