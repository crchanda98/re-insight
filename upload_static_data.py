import requests

import pandas as pd
import requests

# Read the CSV data

CONFIG_PATH = os.getenv("WEATHER_CONFIG", "reinsight_config.yml")
with open(CONFIG_PATH, "r") as f:
    config = yaml.safe_load(f)


df = pd.read_csv(
    config["static_data_path"]
)

try:
    # Convert DataFrame to list of dicts for the payload
    # Drop rows where plant_name is NaN, assuming it is the unique identifier
    if "plant_name" in df.columns:
        df = df.dropna(subset=["plant_name"])
        
    import numpy as np
    df = df.replace({np.nan: None})
    
    # Handle list-like columns correctly for DuckDB array types
    import ast
    import re
    for col in df.columns:
        if df[col].dtype == object:
            try:
                # Pre-process malformed JSON like "{turbine: 100}" to "{\"turbine\": 100}"
                def fix_json_quotes(val):
                    if isinstance(val, str) and (val.startswith("{") or val.startswith("[")):
                        # Add double quotes around unquoted alphanumeric keys
                        val = re.sub(r'([{,]\s*)([A-Za-z0-9_]+)(\s*:)', r'\1"\2"\3', val)
                        # Replace single quotes with double quotes
                        val = val.replace("'", '"')
                    return val
                
                df[col] = df[col].apply(fix_json_quotes)
                # Try to safely evaluate string representations of lists
                df[col] = df[col].apply(lambda x: ast.literal_eval(x) if isinstance(x, str) and (x.startswith("[") or x.startswith("{")) else x)
            except (ValueError, SyntaxError, Exception):
                pass
                
    # Also explicitly convert regulation_bands to float lists if they are numbers/lists
    if "regulation_bands" in df.columns:
        def convert_to_float_list(val):
            if val is None:
                return None
            if isinstance(val, (list, tuple)):
                return [float(v) for v in val]
            if isinstance(val, (int, float)):
                return [float(val)]
            if isinstance(val, str):
                try:
                    # try to parse "[-5, 5]" or similar
                    import json
                    parsed = json.loads(val.replace("'", '"'))
                    if isinstance(parsed, list):
                        return [float(v) for v in parsed]
                    return [float(parsed)]
                except:
                    pass
            return val
        df["regulation_bands"] = df["regulation_bands"].apply(convert_to_float_list)

    payload_data = df.to_dict(orient="records")

    url = "http://127.0.0.1:5000/static_table/push"
    payload = {"data": payload_data}

    print(f"Pushing {len(payload_data)} records...")
    response = requests.post(url, json=payload)

    print(f"Status Code: {response.status_code}")
    try:
        print("Response:", response.json())
    except:
        print("Response Context:", response.text)
        
except FileNotFoundError:
    print(f"Could not find file at {csv_path}")
except Exception as e:
    print(f"Error occurred: {e}")
