import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import yaml
import os
from urllib.parse import quote as urlquote
from sqlalchemy import create_engine
import utils
import extra_streamlit_components as stx
import warnings
import numpy as np

warnings.filterwarnings(action="ignore")

CONFIG_PATH = os.getenv("WEATHER_CONFIG", "reinsight_config.yml")
with open(CONFIG_PATH, "r") as f:
    CONFIG = yaml.safe_load(f)

REQUIRE_LOGIN = CONFIG["dashboard"]["login"]
CREDENTIALS = CONFIG["dashboard"]["credentials"]
FORECAST_MODELS = CONFIG["internal_models"]

# Page config for better appearance
st.set_page_config(
    page_title="Measurement Dashboard",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Authentication
if "authenticated" not in st.session_state:
    st.session_state.authenticated = False


def get_cookie_manager():
    return stx.CookieManager()


cookie_manager = get_cookie_manager()
if REQUIRE_LOGIN:
    if "authenticated" not in st.session_state:
        st.session_state.authenticated = False
    cookie_val = cookie_manager.get(cookie="authenticated")
    if cookie_val == "True":
        st.session_state.authenticated = True
    if not st.session_state.authenticated:
        st.title("⚡ Renew Analytics")
        with st.form("login_form"):
            st.subheader("Please login to continue")
            username = st.text_input("Username")
            password = st.text_input("Password", type="password")
            submit_button = st.form_submit_button("Login")
            if submit_button:
                if username in CREDENTIALS and CREDENTIALS[username] == password:
                    cookie_manager.set("authenticated", "True", key="set_auth_cookie")
                    st.session_state.authenticated = True
                    st.rerun()
                else:
                    st.error("Invalid username or password")
        st.stop()


# Your existing logout function
def logout():
    try:
        # Clear cookies safely
        if "authenticated" in cookie_manager.get_all():
            cookie_manager.delete("authenticated", key="del_auth_cookie")
    except Exception:
        pass

    # Reset auth states
    st.session_state.authenticated = False

    # TRICK: Instead of st.session_state.clear(), manually clear keys
    # or let st.rerun() reset the app state cleanly.
    for key in list(st.session_state.keys()):
        if key != "del_auth_cookie":  # Keep the cookie widget key safe during the rerun
            del st.session_state[key]

    st.rerun()


# # --- SIDEBAR NAVIGATION & LOGOUT ---
# Custom CSS for a more premium look
st.markdown(
    """
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
""",
    unsafe_allow_html=True,
)


@st.cache_resource
def get_db_connection():
    db_cred = CONFIG["db_cred"]
    engine = create_engine(
        f"postgresql://{db_cred['user_name']}:%s@{db_cred['user_ip']}:{db_cred['user_port']}/{db_cred['db_name']}"
        % urlquote(db_cred["user_passwd"])
    )
    db_columns = CONFIG["db_columns"]

    db_con = utils.DBcon(con=engine, db_schema=db_columns)
    return db_con


db_con = get_db_connection()


@st.cache_data(ttl=60)
def get_static_data_cached():
    global db_con
    try:
        return db_con.get_static_data()
    except Exception as e:
        st.error(f"Failed to fetch static data: {e}")
        return pd.DataFrame()


def penalty_kpi(sch, act, avc, regulation, ppa=5):
    """
    Calculate DSM penalties and losses based on actual and forecasted power data.
    """
    penalty = utils.get_dsm_regulation(
        actual=act, forecast=sch, avc_kw=avc * 1000, ppa_rate=ppa, regulation=regulation
    )
    DSM_Loss = penalty["Over_Injection_DSM_Penalty_Rs"]
    Revenue_Loss = penalty["Under_Injection_DSM_Penalty_Rs"]
    Total_Loss = penalty["Total_Statutory_DSM_Penalty_Rs"]
    DSM_impact = penalty["DSM_Impact_Rs_per_Actual_kWh"]
    Loss_percentage = penalty["Total_loss_percentage"]  # .mean().round(2)
    return DSM_Loss, Revenue_Loss, Total_Loss, DSM_impact, Loss_percentage


@st.cache_data
def convert_for_download(df):
    return df.to_csv().encode("utf-8")


def measurement_page():
    global db_con
    st.title("⚡ DSM Dashboard")
    st.markdown(
        "View and analyze historical measurement data across various plants.",
        unsafe_allow_html=True,
    )

    df_static = get_static_data_cached()
    plants = (
        sorted(df_static["plant_name"].dropna().unique().tolist())
        if not df_static.empty
        else []
    )

    # Main Area Configuration Header using Columns in an Expander
    with st.expander("🎛️ Filter Configuration", expanded=True):
        if not plants:
            st.warning("⚠️ No plants fetched. Ensure database connection is valid.")

        # Row 1
        r1_col1, r1_col2, r1_col3 = st.columns(3)
        with r1_col1:
            selected_plant = st.selectbox(
                "Select Plant", options=[""] + (plants if plants else [])
            )
        with r1_col2:
            start_date = st.date_input(
                "Start Date", value=datetime.today() - timedelta(days=7)
            )
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
                FORECAST_MODELS,
            )
        with r2_col3:
            st.text_input("Model Name Input", value="intraday_wind")

        # Row 3
        r3_col1, r3_col2, r3_col3 = st.columns(3)
        with r3_col1:
            fct_lag_hours = st.number_input(
                "Forecast Lag (Hours)", min_value=0, max_value=72, value=2, step=1
            )
        with r3_col2:
            regulation = st.selectbox(
                "Regulation",
                ("CTU-RTM", "CTU-SOLAR", "CTU-WIND", "MP"),
            )
        with r3_col3:
            ppa_rate_input = st.number_input(
                "PPA Rate", min_value=0.0, value=5.0, step=0.1
            )

        # Extract AVC from static data if available
        avc_static = 100.0
        if selected_plant and not df_static.empty:
            plant_static = df_static[df_static["plant_name"] == selected_plant]
            if not plant_static.empty:
                for col in ["avc", "capacity", "installed_capacity", "plant_capacity"]:
                    if col in plant_static.columns and pd.notna(
                        plant_static.iloc[0][col]
                    ):
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
                    df_meas = db_con.get_meas_data(
                        plant=selected_plant, start_date=start_dt, end_date=end_dt
                    )
                    total_power_generated = df_meas["active_power"].sum() / 4 / 1000
                    total_revenue = (
                        total_power_generated * 1000 * ppa_rate_input / 100000
                    )

                    df_schedule = db_con.get_fct_data(
                        plant=selected_plant,
                        fct_src="qca",
                        model_name="schedule",
                        start_date=start_dt,
                        end_date=end_dt,
                    )

                    df_schedule = df_schedule.rename(
                        {"active_power": "schedule"}, axis=1
                    )
                    df_schedule["schedule"] *= 1000
                    df_schedule["forecast_time"] = (
                        pd.to_datetime(df_schedule["forecast_time"], utc=True)
                        .dt.tz_convert("Asia/Kolkata")
                        .dt.tz_localize(None)
                    )
                    df_schedule = df_schedule.set_index("forecast_time")
                    df_fct = pd.DataFrame()
                    if fct_src and model_name:
                        try:
                            df_fct = db_con.get_fct_data(
                                plant=selected_plant,
                                fct_src=fct_src,
                                model_name=model_name,
                                start_date=start_dt,
                                end_date=end_dt,
                            )
                        except Exception as e:
                            st.warning(f"Could not fetch forecast data: {e}")

                    # Process Measurement Data
                    if (
                        df_meas is not None
                        and not df_meas.empty
                        and "record_time" in df_meas.columns
                    ):
                        df_meas["record_time"] = (
                            pd.to_datetime(df_meas["record_time"], utc=True)
                            .dt.tz_convert("Asia/Kolkata")
                            .dt.tz_localize(None)
                        )
                        df_meas = df_meas.set_index("record_time")
                        df_meas = df_meas.sort_index()
                        if "active_power" in df_meas.columns:
                            df_meas = df_meas.rename(
                                columns={"active_power": "meas_active_power"}
                            )

                    # Process Forecast Data
                    if (
                        df_fct is not None
                        and not df_fct.empty
                        and "forecast_time" in df_fct.columns
                    ):
                        df_fct["forecast_time"] = (
                            pd.to_datetime(df_fct["forecast_time"], utc=True)
                            .dt.tz_convert("Asia/Kolkata")
                            .dt.tz_localize(None)
                        )
                        if "prediction_time" in df_fct.columns:
                            df_fct["prediction_time"] = (
                                pd.to_datetime(df_fct["prediction_time"], utc=True)
                                .dt.tz_convert("Asia/Kolkata")
                                .dt.tz_localize(None)
                            )
                            df_fct = utils.filter_forecast_by_regulation(
                                df_fct, regulation=regulation
                            )
                            df_fct = df_fct.sort_values(
                                by=["forecast_time", "prediction_time"]
                            )
                            df_fct = df_fct.drop_duplicates(
                                subset=["forecast_time"], keep="last"
                            )
                        df_fct = df_fct.set_index("forecast_time")
                        df_fct = df_fct[["active_power"]]
                        df_fct = df_fct.resample("15min").interpolate("linear")
                        df_fct = df_fct.sort_index()
                        if "active_power" in df_fct.columns:
                            df_fct = df_fct.rename(
                                columns={"active_power": "fct_active_power"}
                            )

                    # Combine Data
                    combined_df = pd.DataFrame()
                    if (df_meas is not None and not df_meas.empty) and (
                        df_fct is not None and not df_fct.empty
                    ):
                        combined_df = df_meas.join(
                            df_fct[["fct_active_power"]], how="outer"
                        )
                        combined_df = combined_df.join(
                            df_schedule[["schedule"]], how="outer"
                        )
                    elif df_meas is not None and not df_meas.empty:
                        combined_df = df_meas
                    elif df_fct is not None and not df_fct.empty:
                        combined_df = df_fct

                    if combined_df.empty:
                        st.info(
                            "No measurements or forecast found for the selected time range."
                        )
                    else:
                        df_score = pd.DataFrame()
                        for imodel in ["fct_active_power", "schedule"]:
                            df_nona = combined_df.dropna(
                                subset=["meas_active_power", imodel]
                            )
                            df_nona = df_nona[["meas_active_power", imodel]]
                            (
                                dsm_loss,
                                revenue_loss,
                                total_loss,
                                dsm_impact,
                                loss_percentage,
                            ) = penalty_kpi(
                                sch=df_nona[imodel],
                                act=df_nona["meas_active_power"],
                                avc=avc_static,
                                ppa=ppa_rate_input,
                                regulation=regulation,
                            )
                            df_score.loc[imodel, "DSM Impact (ps/kWh)"] = (
                                dsm_impact * 100
                            )
                            df_score.loc[imodel, "DSM Loss (Lacs)"] = dsm_loss / 1e5
                            df_score.loc[imodel, "Revenue Loss (Lacs)"] = (
                                revenue_loss / 1e5
                            )
                            df_score.loc[imodel, "Total Loss (Lacs)"] = total_loss / 1e5
                            df_score.loc[imodel, "Impact (%)"] = loss_percentage
                            df_score = df_score.round(2)

                        st.markdown("---")

                        metrics_col1, metrics_col2, metrics_col3, metrics_col4 = (
                            st.columns(4)
                        )
                        with metrics_col1:
                            st.metric("Total Records", len(combined_df))

                        with metrics_col2:
                            st.metric("Plant Capacity (MW)", avc_static)
                        if "fct_active_power" in combined_df.columns:
                            with metrics_col3:
                                st.metric(
                                    "Power Generated (MWh)",
                                    f"{total_power_generated:.2f}",
                                )

                        with metrics_col4:
                            st.metric("Total Revenue (Lacs)", f"{total_revenue:.2f}")

                        st.markdown("### 📋 Performance Score")
                        st.dataframe(df_score, use_container_width=True)

                        st.markdown("### 📈 Power Comparison (Meas vs Fct)")
                        cols_to_plot = []
                        if "meas_active_power" in combined_df.columns:
                            cols_to_plot.append("meas_active_power")
                        if "fct_active_power" in combined_df.columns:
                            cols_to_plot.append("fct_active_power")
                        cols_to_plot.append("schedule")
                        if not cols_to_plot:
                            numeric_cols = combined_df.select_dtypes(
                                include=["number"]
                            ).columns
                            cols_to_plot = [
                                c for c in numeric_cols if c not in ["plant_id", "id"]
                            ]

                        if cols_to_plot:
                            color_mapping = {
                                "meas_active_power": "#2ecc71",  # Green
                                "fct_active_power": "#e74c3c",  # Red
                                "schedule": "#f1c40f",  # Yellow
                            }
                            colors = [
                                color_mapping.get(c, "#3498db") for c in cols_to_plot
                            ]
                            try:
                                st.line_chart(
                                    combined_df[cols_to_plot],
                                    use_container_width=True,
                                    color=colors,
                                )
                            except TypeError:
                                st.line_chart(
                                    combined_df[cols_to_plot], use_container_width=True
                                )
                        else:
                            st.warning("No numeric columns found to plot.")

                        combined_df = combined_df.sort_index(ascending=False)
                        st.markdown("### 📋 Combined Raw Data")
                        st.dataframe(
                            combined_df[cols_to_plot] if cols_to_plot else combined_df,
                            use_container_width=True,
                        )

                        st.download_button(
                            label="📥 Download Raw Data (CSV)",
                            data=convert_for_download(
                                combined_df[cols_to_plot]
                                if cols_to_plot
                                else combined_df
                            ),
                            file_name=f"{selected_plant}_raw_data_{start_date}_{end_date}.csv",
                            mime="text/csv",
                        )

                except Exception as e:
                    st.error(f"Database Query Error: {e}")
    elif not selected_plant:
        # Initial state
        st.info(
            "👆 Please select a plant and date range from the configuration panel above, then click 'Fetch Data'."
        )


def logging_page():
    global db_con
    r1_col1, r1_col2, r1_col3 = st.columns(3)
    r2_col1, r2_col2 = st.columns(2)
    with r1_col1:
        selected_script = st.selectbox(
            "Select Script",
            options=[
                "aggregator.py",
                "da_wind.py",
                "fct_dispatch.py",
                "intraday_wind_om_rf.py",
                "intraday_wind_v2.py",
                "ncm_data_pull.py",
                "pull_schedule.py",
            ],
        )
    with r1_col2:
        start_date = st.date_input(
            "Start Date", value=datetime.today() - timedelta(days=2)
        )
    with r1_col3:
        end_date = st.date_input("End Date", value=datetime.today())
    with r2_col1:
        log_type = st.radio(
            "Log Type", options=["all", "info", "error", "success"], horizontal=True
        )
    df_log = db_con.get_log_data(
        script=selected_script, start_date=start_date, end_date=end_date
    )
    df_log = df_log.sort_values(by="created_at", ascending=False)
    if log_type is not "all":
        df_log = df_log[df_log["log_type"] == log_type]
    st.dataframe(df_log, use_container_width=True)


pages = {
    "DSM Dashboard": measurement_page,
    "Log Dashboard": logging_page,
}

# --- 2. Sidebar Navigation (Top of Sidebar) ---
st.sidebar.title("Navigation")

# Filter your pages dict based on user role here first if needed!
# (e.g., allowed_pages = {k: v for k, v in pages.items() if k in user_allowed_list})

page = st.sidebar.radio("Go to", list(pages.keys()))

# --- 3. Push Logout Button to the Absolute Bottom ---
# --- 4. Render the Active Page (Main Body) ---
pages[page]()

# --- Push Logout Button to the Absolute Bottom ---
if REQUIRE_LOGIN:
    st.sidebar.markdown(
        """
        <style>
            [data-testid="stSidebarUserContent"] {
                display: flex;
                flex-direction: column;
                height: 100vh;
            }
            .logout-container {
                margin-top: auto;
                padding-bottom: 20px;
            }
        </style>
        """,
        unsafe_allow_html=True,
    )

    with st.sidebar.container():
        st.markdown('<div class="logout-container"></div>', unsafe_allow_html=True)

        # ADD A UNIQUE KEY HERE 👇
        st.button(
            "🔓 Logout",
            on_click=logout,
            use_container_width=True,
            key="sidebar_logout_button_unique",
        )
