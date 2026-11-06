import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import numpy as np
import os
import yaml
from sqlalchemy import create_engine
from urllib.parse import quote_plus as urlquote
import utils

CONFIG_PATH = os.getenv("WEATHER_CONFIG", "reinsight_config.yml")
with open(CONFIG_PATH, "r") as f:
    CONFIG = yaml.safe_load(f)


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


def run_timeseries():
    st.title("Time-Series Anomaly Detection")

    if "original_df" not in st.session_state:
        st.session_state.original_df = None
    if "df" not in st.session_state:
        st.session_state.df = None

    db = get_db_connection()

    st.sidebar.header("Data Fetch Configuration")
    plant_names = db.df_static["plant_name"].tolist()
    selected_plant = st.sidebar.selectbox(
        "Select Plant", options=plant_names, key="ts_plant"
    )

    start_date = st.sidebar.date_input("Start Date", key="ts_start")
    end_date = st.sidebar.date_input("End Date", key="ts_end")

    if st.sidebar.button("Fetch Data", type="primary", key="ts_fetch"):
        start_str = f"{start_date} 00:00:00+05:30"
        end_str = f"{end_date} 23:59:59+05:30"
        raw_df = db.get_meas_data(
            plant=selected_plant, start_date=start_str, end_date=end_str, source="raw"
        )
        clean_df = db.get_meas_data(
            plant=selected_plant, start_date=start_str, end_date=end_str, source="clean"
        )

        if not raw_df.empty:
            working_df = raw_df.copy()
            if not clean_df.empty:
                working_df = working_df.set_index("record_time")
                clean_df = clean_df.set_index("record_time")

                common_idx = working_df.index.intersection(clean_df.index)
                working_df.loc[common_idx] = clean_df.loc[common_idx]

                working_df = working_df.reset_index()

            for df_target in (raw_df, working_df):
                df_target["record_time"] = pd.to_datetime(df_target["record_time"])
                tz_converted = (
                    df_target["record_time"].dt.tz_convert("Asia/Kolkata")
                    if df_target["record_time"].dt.tz
                    else df_target["record_time"]
                    .dt.tz_localize("UTC")
                    .dt.tz_convert("Asia/Kolkata")
                )
                df_target["Date"] = tz_converted.dt.date

            raw_df = raw_df.sort_values("record_time").reset_index(drop=True)
            working_df = working_df.sort_values("record_time").reset_index(drop=True)

            st.session_state.original_df = raw_df
            st.session_state.df = working_df
            st.sidebar.success(f"Successfully loaded {len(working_df)} records.")
        else:
            st.sidebar.error("No records found for the specified parameters.")

    if st.session_state.df is not None:
        df = st.session_state.df
        original_df = st.session_state.original_df

        col1, col2, col3, col4, col5 = st.columns([2, 1, 1, 1, 1])

        available_dates = list(original_df["Date"].dropna().unique())

        if "ts_date_sel" not in st.session_state:
            st.session_state.ts_date_sel = available_dates[0]

        def prev_day():
            current_date = st.session_state.ts_date_sel
            if current_date in available_dates:
                idx = available_dates.index(current_date)
                if idx > 0:
                    st.session_state.ts_date_sel = available_dates[idx - 1]

        def next_day():
            current_date = st.session_state.ts_date_sel
            if current_date in available_dates:
                idx = available_dates.index(current_date)
                if idx < len(available_dates) - 1:
                    st.session_state.ts_date_sel = available_dates[idx + 1]

        col1, col2, col3, col4, col5 = st.columns([3, 1.2, 1.2, 1, 1])

        with col1:
            ac1, ac2, ac3 = st.columns([1, 4, 1])
            with ac1:
                st.write("")
                st.write("")
                st.button(
                    "⬅️", on_click=prev_day, use_container_width=True, key="btn_prev"
                )

            with ac2:
                selected_date = st.selectbox(
                    "Select Date", options=available_dates, key="ts_date_sel"
                )

            with ac3:
                st.write("")
                st.write("")
                st.button(
                    "➡️️", on_click=next_day, use_container_width=True, key="btn_next"
                )

            daily_df = df[df["Date"] == selected_date]
            daily_original_df = original_df[original_df["Date"] == selected_date]

        with col2:
            x_col = st.selectbox("X-axis", ["record_time"], key="ts_x_col")

        with col3:
            y_col = st.selectbox(
                "Metric to Clean", ["active_power", "ghi"], key="ts_y_col"
            )

        with col4:
            st.write("")
            st.write("")
            csv_export = df.drop(columns=["Date"]).to_csv(index=False).encode("utf-8")
            st.download_button(
                label="Export CSV",
                data=csv_export,
                file_name=f"{selected_plant}_cleaned_data.csv",
                mime="text/csv",
                use_container_width=True,
                key="ts_export_btn",
            )

        with col5:
            st.write("")
            st.write("")
            if st.button(
                "Save to DB",
                type="primary",
                use_container_width=True,
                key="ts_save_btn",
            ):
                try:
                    df_to_save = df.drop(
                        columns=["Date", "plant_name"], errors="ignore"
                    ).copy()
                    df_to_save = df_to_save.set_index(["plant_id", "record_time"])
                    db.push_meas_data(idf=df_to_save, source="clean")
                    st.success("Successfully pushed cleaned dataset to database.")
                except Exception as e:
                    st.error(f"Database transaction failed: {e}")

        st.divider()

        total_original = original_df[y_col].notna().sum()
        total_current = df[y_col].notna().sum()
        total_removed = total_original - total_current
        global_clean_pct = (
            (total_removed / total_original) * 100 if total_original > 0 else 0
        )

        daily_original = daily_original_df[y_col].notna().sum()
        daily_current = daily_df[y_col].notna().sum()
        daily_removed = daily_original - daily_current
        daily_clean_pct = (
            (daily_removed / daily_original) * 100 if daily_original > 0 else 0
        )

        st.subheader(f"Data Quality Metrics: {y_col.replace('_', ' ').title()}")
        m1, m2, m3, m4 = st.columns(4)
        m1.metric(
            "Anomalies Removed (Selected Date)",
            f"{daily_removed}",
            f"-{daily_clean_pct:.2f}%",
        )
        m2.metric(
            "Valid Data Points (Selected Date)", f"{daily_current} / {daily_original}"
        )
        m3.metric(
            "Total Anomalies Removed (Global)",
            f"{total_removed}",
            f"-{global_clean_pct:.2f}%",
        )
        m4.metric(
            "Total Valid Data Points (Global)", f"{total_current} / {total_original}"
        )

        st.divider()

        st.subheader(f"Interactive Selection: {y_col.replace('_', ' ').title()}")
        fig = px.scatter(daily_df, x=x_col, y=y_col)
        fig.update_layout(dragmode="lasso", margin=dict(t=10, b=10, l=10, r=10))

        event = st.plotly_chart(
            fig,
            on_select="rerun",
            selection_mode="lasso",
            key=f"chart_{selected_date}_{y_col}",
            use_container_width=True,
        )

        selected_indices = event.selection["point_indices"]

        if selected_indices:
            st.warning(
                f"Selected {len(selected_indices)} anomalous records in {y_col.replace('_', ' ').title()}."
            )

            if st.button(
                f"Remove Anomalies from {y_col.replace('_', ' ').title()}",
                type="primary",
                key="ts_rmv_anomalies",
            ):
                real_indices_to_drop = daily_df.iloc[selected_indices].index
                st.session_state.df.loc[real_indices_to_drop, y_col] = np.nan
                st.rerun()

        st.divider()

        st.subheader("Dataset Comparison Overview")
        st.caption(
            f"Visual distribution of valid records (green) versus removed anomalies (red) for {y_col.replace('_', ' ').title()}."
        )

        comp_fig = go.Figure()

        comp_fig.add_trace(
            go.Scatter(
                x=daily_original_df[x_col],
                y=daily_original_df[y_col],
                mode="markers",
                name="Baseline Data",
                marker=dict(color="#d62728", size=8, opacity=0.7),
            )
        )

        clean_plot_df = daily_df.dropna(subset=[y_col])
        comp_fig.add_trace(
            go.Scatter(
                x=clean_plot_df[x_col],
                y=clean_plot_df[y_col],
                mode="markers",
                name="Valid Data",
                marker=dict(color="#2ca02c", size=5),
            )
        )

        comp_fig.update_layout(
            hovermode="x unified",
            margin=dict(l=0, r=0, t=30, b=0),
            legend=dict(
                orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1
            ),
        )
        st.plotly_chart(comp_fig, use_container_width=True, key="ts_compare_chart")
    else:
        st.info(
            "Configure parameters in the sidebar to fetch data and begin anomaly detection."
        )


if __name__ == "__main__":
    run_timeseries()
