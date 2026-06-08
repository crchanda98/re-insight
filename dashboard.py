from logging import warning
from sqlalchemy.util import warn
import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import yaml
import os
from urllib.parse import quote as urlquote
from sqlalchemy import create_engine
import numpy as np
import utils
import warnings
# import solar_dashboard
# from solar_dashboard import sample_page

warnings.filterwarnings(action="ignore")

# Page config for better appearance
st.set_page_config(
    page_title="Measurement Dashboard", 
    page_icon="⚡", 
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for a more premium look
st.markdown("""
<style>
    .main {
        background-color: #0e1117;
        color: #fafafa;
    }
    .stButton>button {
        background-color: #4CAF50;
        color: white;
        border-radius: 5px;
        border: none;
        padding: 10px 24px;
        transition: all 0.3s;
    }
    .stButton>button:hover {
        background-color: #45a049;
        box-shadow: 0 4px 8px 0 rgba(0,0,0,0.2);
        transform: scale(1.02);
    }
    .stDataFrame {
        border-radius: 10px;
        overflow: hidden;
    }
    h1 {
        color: #4CAF50;
        font-family: 'Inter', sans-serif;
    }
</style>
""", unsafe_allow_html=True)

@st.cache_resource
def get_db_connection():
    CONFIG_PATH = os.getenv("WEATHER_CONFIG", "reinsight_config.yml")
    with open(CONFIG_PATH, "r") as f:
        config = yaml.safe_load(f)

    db_cred = config["db_cred"]

    engine = create_engine(
        f"postgresql://{db_cred['user_name']}:%s@{db_cred['user_ip']}:{db_cred['user_port']}/{db_cred['db_name']}"
        % urlquote(db_cred["user_passwd"])
    )
    db_columns = config["db_columns"]

    db_con = utils.DBcon(con=engine, db_schema=db_columns)
    return db_con


@st.cache_data(ttl=60)
def get_static_data_cached():
    try:
        db_con = get_db_connection()
        return db_con.get_static_data()
    except Exception as e:
        st.error(f"Failed to fetch static data: {e}")
        return pd.DataFrame()

def measurement_page():
    st.title("⚡ Plant Measurement Dashboard")
    st.markdown("View and analyze historical measurement data across various plants.", unsafe_allow_html=True)

    df_static = get_static_data_cached()
    plants = sorted(df_static['plant_name'].dropna().unique().tolist()) if not df_static.empty else []

    # Main Area Configuration Header using Columns in an Expander
    with st.expander("🎛️ Filter Configuration", expanded=True):
        if not plants:
            st.warning("⚠️ No plants fetched. Ensure database connection is valid.")
        
        # Row 1
        r1_col1, r1_col2, r1_col3 = st.columns(3)
        with r1_col1:
            selected_plant = st.selectbox("Select Plant", options=[""] + (plants if plants else []))
        with r1_col2:
            start_date = st.date_input("Start Date", value=datetime.today() - timedelta(days=7))
        with r1_col3:
            end_date = st.date_input("End Date", value=datetime.today())
            
        # Row 2
        r2_col1, r2_col2, r2_col3 = st.columns(3)
        with r2_col1:
            fct_src = st.selectbox(
                "Forecast Source",
                ("inhouse",),
            )
        with r2_col2:
            model_name = st.selectbox(
                "Model Name",
                ("intraday_wind", "om_xgb"),
            )
        with r2_col3:
            st.text_input("Model Name Input", value="intraday_wind")
            
        # Row 3
        r3_col1, r3_col2, r3_col3 = st.columns(3)
        with r3_col1:
            fct_lag_hours = st.number_input("Forecast Lag (Hours)", min_value=0, max_value=72, value=2, step=1)
        with r3_col2:
            regulation = st.selectbox(
                "Regulation",
                ("CTU-RTM", "CTU", "MP"),
            )
        with r3_col3:
            ppa_rate_input = st.number_input("PPA Rate", min_value=0.0, value=5.0, step=0.1)

        # Extract AVC from static data if available
        avc_static = 100.0
        if selected_plant and not df_static.empty:
            plant_static = df_static[df_static['plant_name'] == selected_plant]
            if not plant_static.empty:
                for col in ['avc', 'capacity', 'installed_capacity', 'plant_capacity']:
                    if col in plant_static.columns and pd.notna(plant_static.iloc[0][col]):
                        avc_static = float(plant_static.iloc[0][col])
                        break

        fetch_btn = st.button("Fetch Data", use_container_width=True)

    if fetch_btn:
        if not selected_plant:
            st.error("Please select a plant from the dropdown.")
        elif start_date > end_date:
            st.error("Start Date cannot be after End Date.")
        else:
            start_dt = f"{start_date.strftime('%Y-%m-%d')}T00:00:00"
            end_dt = f"{end_date.strftime('%Y-%m-%d')}T23:59:59"
            
            with st.spinner(f"Fetching data for {selected_plant}..."):
                try:
                    db_con = get_db_connection()
                    df_meas = db_con.get_meas_data(plant=selected_plant, start_date=start_dt, end_date=end_dt)
                    df_schedule = db_con.get_fct_data(
                        plant=selected_plant, 
                        fct_src="qca", 
                        model_name="schedule", 
                        start_date=start_dt, 
                        end_date=end_dt
                    )
                    df_schedule = df_schedule.rename({"active_power": "schedule"}, axis=1)
                    df_schedule["forecast_time"] = pd.to_datetime(df_schedule["forecast_time"], utc=True).dt.tz_convert("Asia/Kolkata").dt.tz_localize(None)
                    df_schedule = df_schedule.set_index("forecast_time")
                    df_fct = pd.DataFrame()
                    if fct_src and model_name:
                        try:
                            df_fct = db_con.get_fct_data(
                                plant=selected_plant, 
                                fct_src=fct_src, 
                                model_name=model_name, 
                                start_date=start_dt, 
                                end_date=end_dt
                            )
                        except Exception as e:
                            st.warning(f"Could not fetch forecast data: {e}")
                    
                    # Process Measurement Data
                    if df_meas is not None and not df_meas.empty and "record_time" in df_meas.columns:
                        df_meas["record_time"] = pd.to_datetime(df_meas["record_time"], utc=True).dt.tz_convert("Asia/Kolkata").dt.tz_localize(None)
                        df_meas = df_meas.set_index("record_time")
                        df_meas = df_meas.sort_index()
                        if "active_power" in df_meas.columns:
                            df_meas = df_meas.rename(columns={"active_power": "meas_active_power"})
                    
                    # Process Forecast Data
                    if df_fct is not None and not df_fct.empty and "forecast_time" in df_fct.columns:
                        df_fct["forecast_time"] = pd.to_datetime(df_fct["forecast_time"], utc=True).dt.tz_convert("Asia/Kolkata").dt.tz_localize(None)
                        if "prediction_time" in df_fct.columns:
                            df_fct["prediction_time"] = pd.to_datetime(df_fct["prediction_time"], utc=True).dt.tz_convert("Asia/Kolkata").dt.tz_localize(None)
                            df_fct = utils.filter_forecast_by_regulation(df_fct, regulation=regulation)
                            df_fct = df_fct.sort_values(by=["forecast_time", "prediction_time"])
                            df_fct = df_fct.drop_duplicates(subset=["forecast_time"], keep="last")
                        df_fct = df_fct.set_index("forecast_time")
                        df_fct = df_fct.sort_index()
                        if "active_power" in df_fct.columns:
                            df_fct = df_fct.rename(columns={"active_power": "fct_active_power"})

                    # Combine Data
                    combined_df = pd.DataFrame()
                    if (df_meas is not None and not df_meas.empty) and (df_fct is not None and not df_fct.empty):
                        combined_df = df_meas.join(df_fct[["fct_active_power"]], how="outer")
                        combined_df = combined_df.join(df_schedule[["schedule"]], how="outer")
                    elif df_meas is not None and not df_meas.empty:
                        combined_df = df_meas
                    elif df_fct is not None and not df_fct.empty:
                        combined_df = df_fct

                    if combined_df.empty:
                        st.info("No measurements or forecast found for the selected time range.")
                    else:
                        if "meas_active_power" in combined_df.columns and "fct_active_power" in combined_df.columns:
                            valid_mask = combined_df["meas_active_power"].notna() & combined_df["fct_active_power"].notna()
                            penalty_df = utils.calculate_ap_dsm_directional_losses(
                                actual=combined_df.loc[valid_mask, "meas_active_power"] / 1000,
                                scheduled=combined_df.loc[valid_mask, "fct_active_power"] / 1000,
                                avc=avc_static,
                                ppa_rate=ppa_rate_input
                            )
                            combined_df.loc[valid_mask, "DSM_Loss"] = penalty_df["DSM_Loss_UnderInjection_Rs"]
                            combined_df.loc[valid_mask, "Revenue_Loss"] = penalty_df["Revenue_Loss_OverInjection_Rs"]
                            combined_df.loc[valid_mask, "Total_Loss"] = penalty_df["Total_Financial_Loss_Rs"]
                            combined_df.loc[valid_mask, "DSM_impact"] = penalty_df["Loss_Impact_Rs_per_kWh"]

                        st.markdown("---")
                        
                        metrics_col1, metrics_col2, metrics_col3 = st.columns(3)
                        with metrics_col1:
                            st.metric("Total Records", len(combined_df))
                        
                        with metrics_col2:
                            st.metric("Plant Capacity", avc_static)
                        if "fct_active_power" in combined_df.columns:
                            with metrics_col3:
                                st.metric("Avg Fct Power", f"{combined_df['fct_active_power'].mean():.2f}")
                        
                        st.markdown("### 💰 Penalty Summary")
                        p_col1, p_col2, p_col3, p_col4 = st.columns(4)
                        with p_col1:
                            st.metric("Total Loss", f"₹ {combined_df['Total_Loss'].sum():,.2f}")
                        with p_col2:
                            st.metric("DSM Loss", f"{combined_df['DSM_Loss'].sum():.2f}")
                        with p_col3:
                            st.metric("Revenue Loss", f"{combined_df['Revenue_Loss'].mean():.2f}")
                        with p_col4:
                            st.metric("Avg DSM Impact (ps)", f"{combined_df['DSM_impact'].mean():.2f}")
                        
                        st.markdown("### 📈 Power Comparison (Meas vs Fct)")
                        
                        cols_to_plot = []
                        if "meas_active_power" in combined_df.columns:
                            cols_to_plot.append("meas_active_power")
                        if "fct_active_power" in combined_df.columns:
                            cols_to_plot.append("fct_active_power")
                            
                        if not cols_to_plot:
                            numeric_cols = combined_df.select_dtypes(include=['number']).columns
                            cols_to_plot = [c for c in numeric_cols if c not in ["plant_id", "id"]]
                            
                        if cols_to_plot:
                            color_mapping = {
                                "meas_active_power": "#2ecc71", # Green
                                "fct_active_power": "#e74c3c"   # Red
                            }
                            colors = [color_mapping.get(c, "#3498db") for c in cols_to_plot]
                            try:
                                st.line_chart(combined_df[cols_to_plot], use_container_width=True, color=colors)
                            except TypeError:
                                st.line_chart(combined_df[cols_to_plot], use_container_width=True)
                        else:
                            st.warning("No numeric columns found to plot.")
                        
                        combined_df = combined_df.sort_index(ascending=False)
                        st.markdown("### 📋 Combined Raw Data")
                        st.dataframe(combined_df[cols_to_plot] if cols_to_plot else combined_df, use_container_width=True)
                        
                except Exception as e:
                    st.error(f"Database Query Error: {e}")
    elif not selected_plant:
        # Initial state
        st.info("👆 Please select a plant and date range from the configuration panel above, then click 'Fetch Data'.")


pages = {
    "Plant Measurement Dashboard": measurement_page,
    # "Solar Dayahead Forecasting": sample_page
}

st.sidebar.title("Navigation")
page = st.sidebar.radio("Go to", pages.keys())

pages[page]()
