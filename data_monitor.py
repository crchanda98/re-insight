import streamlit as st
import pandas as pd
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode


def _parse_minutes(val):
    if not isinstance(val, str):
        return 0
    parts = val.replace(chr(39), chr(32)).split()
    minutes = 0
    for i, p in enumerate(parts):
        if p == "hour" and i > 0:
            minutes += int(parts[i-1]) * 60
        elif p == "minute" and i > 0:
            minutes += int(parts[i-1])
    return minutes


def _color_time(val, max_green=15, max_yellow=60):
    mins = _parse_minutes(val)
    if mins < max_green:
        return "background-color: #D4EDDA"
    elif mins <= max_yellow:
        return "background-color: #FFF3CD"
    else:
        return "background-color: #F8D7DA"


def _color_time_forecast(val):
    return _color_time(val, max_yellow=30)


def _color_time_model(val):
    return _color_time(val, max_green=720, max_yellow=1440)


def data_monitor_page(db_con):
    st.title("🔄 Data Monitor")
    st.markdown(
        "View the most recent data ingestion runs per plant."
        , unsafe_allow_html=True
    )

    hours = st.selectbox(
        "Lookback Period",
        options=[1, 6, 12, 24, 48],
        index=3,
        help="Select how far back to look for data runs."
    )

    source = st.selectbox(
        "Data Source",
        options=["Measurement Table", "Model Table", "Forecast Table"],
        index=0,
        help="Choose which table to monitor."
    )

    tz = st.selectbox(
        "Timezone",
        options=["Asia/Kolkata", "UTC"],
        index=0,
        help="Select timezone for displaying timestamps."
    )

    if source == "Measurement Table":
        time_cols = ["record_time", "created_at"]
        sql = f"""WITH ranked_records AS (
            SELECT
                plant_id,
                record_time,
                created_at,
                CONCAT(
                    FLOOR(EXTRACT(EPOCH FROM (NOW() - record_time)) / 3600)::int,
                    ' hour ',
                    FLOOR((EXTRACT(EPOCH FROM (NOW() - record_time)) %% 3600) / 60)::int, ' minute'
                ) AS time_diff,
                CONCAT(
                    FLOOR(EXTRACT(EPOCH FROM (NOW() - created_at)) / 3600)::int,
                    ' hour ',
                    FLOOR((EXTRACT(EPOCH FROM (NOW() - created_at)) %% 3600) / 60)::int, ' minute'
                ) AS time_diff_now,
                ROW_NUMBER() OVER (
                    PARTITION BY plant_id
                    ORDER BY record_time DESC
                ) AS run_rank
            FROM re_insight.meas_table
            WHERE created_at >= NOW() - INTERVAL '{hours} hours'
        )
        SELECT
            s.plant_id,
            s.plant_name,
            r.time_diff,
            r.time_diff_now,
            r.record_time,
            r.created_at
        FROM re_insight.static_table s
        LEFT JOIN (
            SELECT * FROM ranked_records WHERE run_rank = 1
        ) r ON s.plant_id = r.plant_id
        ORDER BY s.plant_name
    """
    elif source == "Model Table":
        time_cols = ["prediction_time", "forecast_available_till", "created_at"]
        sql = f"""WITH ranked_records AS (
            SELECT
                plant_id,
                model_name,
                prediction_time,
                forecast_time,
                created_at,
                CONCAT(
                    FLOOR(EXTRACT(EPOCH FROM (NOW() - prediction_time)) / 3600)::int,
                    ' hour ',
                    FLOOR((EXTRACT(EPOCH FROM (NOW() - prediction_time)) %% 3600) / 60)::int, ' minute'
                ) AS time_diff,
                CONCAT(
                    FLOOR(EXTRACT(EPOCH FROM (NOW() - created_at)) / 3600)::int,
                    ' hour ',
                    FLOOR((EXTRACT(EPOCH FROM (NOW() - created_at)) %% 3600) / 60)::int, ' minute'
                ) AS time_diff_now,
                ROW_NUMBER() OVER (
                    PARTITION BY plant_id, model_name
                    ORDER BY prediction_time DESC
                ) AS run_rank
            FROM re_insight.weather_table
            WHERE created_at >= NOW() - INTERVAL '{hours} hours'
        ),
        max_forecasts AS (
            SELECT
                plant_id,
                model_name,
                MAX(forecast_time) AS forecast_available_till
            FROM re_insight.weather_table
            GROUP BY plant_id, model_name
        )
        SELECT
            s.plant_id,
            s.plant_name,
            r.model_name,
            r.time_diff,
            r.time_diff_now,
            r.prediction_time,
            mf.forecast_available_till,
            r.created_at
        FROM re_insight.static_table s
        LEFT JOIN (
            SELECT * FROM ranked_records WHERE run_rank = 1
        ) r ON s.plant_id = r.plant_id
        LEFT JOIN max_forecasts mf
            ON s.plant_id = mf.plant_id AND r.model_name = mf.model_name
        ORDER BY s.plant_name, r.model_name
    """

    else:
        time_cols = ["prediction_time", "forecast_available_till", "created_at"]
        sql = f"""WITH ranked_records AS (
            SELECT
                plant_id,
                model_name,
                prediction_time,
                forecast_time,
                created_at,
                CONCAT(
                    FLOOR(EXTRACT(EPOCH FROM (NOW() - prediction_time)) / 3600)::int,
                    ' hour ',
                    FLOOR((EXTRACT(EPOCH FROM (NOW() - prediction_time)) %% 3600) / 60)::int, ' minute'
                ) AS time_diff,
                CONCAT(
                    FLOOR(EXTRACT(EPOCH FROM (NOW() - created_at)) / 3600)::int,
                    ' hour ',
                    FLOOR((EXTRACT(EPOCH FROM (NOW() - created_at)) %% 3600) / 60)::int, ' minute'
                ) AS time_diff_now,
                ROW_NUMBER() OVER (
                    PARTITION BY plant_id, model_name
                    ORDER BY prediction_time DESC
                ) AS run_rank
            FROM re_insight.forecast_table
            WHERE created_at >= NOW() - INTERVAL '{hours} hours'
        ),
        max_forecasts AS (
            SELECT
                plant_id,
                model_name,
                MAX(forecast_time) AS forecast_available_till
            FROM re_insight.forecast_table
            GROUP BY plant_id, model_name
        )
        SELECT
            s.plant_name,
            r.model_name,
            r.time_diff,
            r.time_diff_now,
            r.prediction_time,
            mf.forecast_available_till,
            r.created_at
        FROM re_insight.static_table s
        LEFT JOIN (
            SELECT * FROM ranked_records WHERE run_rank = 1
        ) r ON s.plant_id = r.plant_id
        LEFT JOIN max_forecasts mf
            ON s.plant_id = mf.plant_id AND r.model_name = mf.model_name
        ORDER BY s.plant_name, r.model_name
    """

    try:
        with db_con.conn.connect() as conn:
            df = pd.read_sql(sql, conn)
        if df.empty:
            st.info("No data found in the lookback period.")
        else:
            for col in time_cols:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], utc=True).dt.tz_convert(tz).dt.tz_localize(None)
            if source == "Measurement Table":
                styled = df.style.map(_color_time, subset=["time_diff", "time_diff_now"])
                st.dataframe(styled, use_container_width=True)
            elif source == "Model Table":
                styled = df.style.map(_color_time_model, subset=["time_diff", "time_diff_now"])
                st.dataframe(styled, use_container_width=True)
            else:
                styled = df.style.map(_color_time_forecast, subset=["time_diff", "time_diff_now"])
                st.dataframe(styled, use_container_width=True)
    except Exception as e:
        st.error(f"Database Query Error: {e}")
