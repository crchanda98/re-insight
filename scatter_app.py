import streamlit as st
import plotly.express as px
import pandas as pd
import numpy as np
import os
import yaml
from sqlalchemy import create_engine
from urllib.parse import quote_plus as urlquote
import utils


@st.cache_resource
def get_db_connection():
    CONFIG_PATH = os.getenv("WEATHER_CONFIG", "reinsight_config.yml")
    with open(CONFIG_PATH, "r") as f:
        config = yaml.safe_load(f)

    db_cred = config.get("reinsight_db_config", config.get("db_cred"))
    engine = create_engine(
        f"postgresql://{db_cred['user_name']}:%s@{db_cred['user_ip']}:{db_cred['user_port']}/{db_cred.get('dbname', db_cred.get('db_name'))}"
        % urlquote(db_cred["user_passwd"])
    )
    db_columns = config.get("db_columns", [])
    db_con = utils.DBcon(con=engine, db_schema=db_columns)
    return db_con


def run_scatter():
    st.title("Multivariate Anomaly Detection (GHI vs. Active Power)")

    if "pipeline_baseline" not in st.session_state:
        st.session_state.pipeline_baseline = None
    if "master_data" not in st.session_state:
        st.session_state.master_data = None

    db = get_db_connection()

    st.sidebar.header("Data Fetch Configuration")

    plant_names = db.df_static["plant_name"].tolist()
    selected_plant = st.sidebar.selectbox(
        "Select Plant", options=plant_names, key="sc_plant"
    )
    start_date = st.sidebar.date_input("Start Date", key="sc_start")
    end_date = st.sidebar.date_input("End Date", key="sc_end")

    if st.sidebar.button("Fetch Data", type="primary", key="sc_fetch"):
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
                working_df.loc[common_idx, ["active_power", "ghi"]] = clean_df.loc[
                    common_idx, ["active_power", "ghi"]
                ]

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

            st.session_state.pipeline_baseline = raw_df.copy()
            st.session_state.master_data = working_df.copy()

            st.sidebar.success(f"Successfully loaded {len(working_df)} records.")
        else:
            st.sidebar.error("No records found.")

    if st.session_state.master_data is not None:
        df = st.session_state.master_data
        baseline_df = st.session_state.pipeline_baseline

        col1, col2, col3, col4 = st.columns([2, 1, 1, 1])

        with col1:
            unique_dates = baseline_df["Date"].unique()
            selected_date = st.selectbox(
                "Select Date for Analysis", unique_dates, key="sc_date_sel"
            )

        with col3:
            st.write("")
            st.write("")
            csv_export = (
                df.drop(columns=["Date", "plant_name"], errors="ignore")
                .to_csv(index=False)
                .encode("utf-8")
            )
            st.download_button(
                label="Export Final CSV",
                data=csv_export,
                file_name=f"{selected_plant}_scatter_cleaned_data.csv",
                mime="text/csv",
                use_container_width=True,
                key="sc_export_btn",
            )

        with col4:
            st.write("")
            st.write("")
            if st.button(
                "Save to Cleaned Database",
                type="primary",
                use_container_width=True,
                key="sc_save_btn",
            ):
                try:
                    df_to_save = df.copy()

                    if "plant_id" not in df_to_save.columns:
                        ist = db.df_static[
                            db.df_static["plant_name"] == selected_plant
                        ].iloc[0]
                        df_to_save["plant_id"] = int(ist["plant_id"])

                    df_to_save = df_to_save.drop(
                        columns=["Date", "plant_name"], errors="ignore"
                    )
                    df_to_save = df_to_save[
                        ["plant_id", "record_time", "active_power", "ghi"]
                    ]

                    df_to_save = df_to_save.set_index(["plant_id", "record_time"])

                    db.push_meas_data(idf=df_to_save, source="clean")

                    st.success(
                        "Successfully committed fully cleaned dataset to database."
                    )
                except Exception as e:
                    st.error(f"Database transaction failed: {e}")

        st.divider()

        day_baseline_df = baseline_df[baseline_df["Date"] == selected_date]
        day_clean_df = df[df["Date"] == selected_date]

        target_cols = ["active_power", "ghi"]

        total_baseline_valid = day_baseline_df[target_cols].notna().all(axis=1).sum()
        total_current_valid = day_clean_df[target_cols].notna().all(axis=1).sum()

        daily_removed = total_baseline_valid - total_current_valid
        daily_clean_pct = (
            (daily_removed / total_baseline_valid) * 100
            if total_baseline_valid > 0
            else 0
        )

        st.subheader("Data Quality Metrics (Scatter Cleanup)")
        m1, m2 = st.columns(2)
        m1.metric(
            "Anomalies Removed (Selected Date)",
            f"{daily_removed}",
            f"-{daily_clean_pct:.2f}%",
        )
        m2.metric(
            "Valid Plot Points Remaining",
            f"{total_current_valid} / {total_baseline_valid}",
        )

        st.divider()

        x_min, x_max = day_baseline_df["ghi"].min(), day_baseline_df["ghi"].max()
        y_min, y_max = (
            day_baseline_df["active_power"].min(),
            day_baseline_df["active_power"].max(),
        )

        x_min = 0 if pd.isna(x_min) else x_min
        y_min = 0 if pd.isna(y_min) else y_min
        x_max = 1000 if pd.isna(x_max) else x_max
        y_max = 1000 if pd.isna(y_max) else y_max

        x_buffer = (x_max - x_min) * 0.05 if x_max != x_min else 10
        y_buffer = (y_max - y_min) * 0.05 if y_max != y_min else 10

        st.subheader("Interactive Scatter Anomaly Selection")

        plot_df = day_clean_df.dropna(subset=["active_power", "ghi"])

        if plot_df.empty:
            st.info(
                "No valid data points available for plotting on this date. Either the data is missing, or it was completely cleaned in the Time-Series app."
            )
        else:
            fig_clean = px.scatter(
                plot_df,
                x="ghi",
                y="active_power",
                color_discrete_sequence=["#1f77b4"],
                hover_data=["record_time"],
                custom_data=[plot_df.index],
            )
            fig_clean.update_layout(
                dragmode="lasso", margin=dict(t=30, b=30, l=30, r=30)
            )
            fig_clean.update_xaxes(
                range=[x_min - x_buffer, x_max + x_buffer],
                title_text="Global Horizontal Irradiance (GHI)",
            )
            fig_clean.update_yaxes(
                range=[y_min - y_buffer, y_max + y_buffer], title_text="Active Power"
            )

            selection_event = st.plotly_chart(
                fig_clean,
                on_select="rerun",
                selection_mode=("lasso", "box"),
                key="sc_lasso_chart",
            )

            selected_points = selection_event.selection.points
            if len(selected_points) > 0:
                indices_to_drop = [point["customdata"][0] for point in selected_points]
                valid_indices_to_drop = [
                    idx for idx in indices_to_drop if idx in df.index
                ]

                if valid_indices_to_drop:
                    st.warning(
                        f"Selected {len(valid_indices_to_drop)} data points. Click below to flag as anomalous (will be set to NaN)."
                    )
                    if st.button(
                        "Remove Selected Anomalies",
                        type="primary",
                        key="sc_rmv_anomalies",
                    ):
                        st.session_state.master_data.loc[
                            valid_indices_to_drop, ["active_power", "ghi"]
                        ] = np.nan
                        st.rerun()

        st.subheader("Dataset Comparison Overview")
        st.caption(
            "Visual distribution of valid records (green) versus removed anomalies (red)."
        )

        comp_df = day_baseline_df.copy()

        valid_mask = day_clean_df[["active_power", "ghi"]].notna().all(axis=1)

        comp_df["Status"] = "Identified Anomaly"
        comp_df.loc[valid_mask, "Status"] = "Valid Data"

        comp_plot_df = comp_df.dropna(subset=["active_power", "ghi"])

        fig_compare = px.scatter(
            comp_plot_df,
            x="ghi",
            y="active_power",
            color="Status",
            color_discrete_map={
                "Valid Data": "#2ca02c",
                "Identified Anomaly": "#d62728",
            },
            hover_data=["record_time"],
        )

        fig_compare.update_layout(
            margin=dict(t=30, b=30, l=30, r=30),
            legend=dict(
                orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1
            ),
        )
        fig_compare.update_xaxes(
            range=[x_min - x_buffer, x_max + x_buffer],
            title_text="Global Horizontal Irradiance (GHI)",
        )
        fig_compare.update_yaxes(
            range=[y_min - y_buffer, y_max + y_buffer], title_text="Active Power"
        )

        st.plotly_chart(fig_compare, use_container_width=True, key="sc_compare_chart")


if __name__ == "__main__":
    run_scatter()
