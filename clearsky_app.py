import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import yaml
import os
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus as urlquote
import utils
import plotly.graph_objects as go
from plotly.subplots import make_subplots

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


def run_clearsky_app():
    st.title("Clear Sky Profile Generator")

    db = get_db_connection()

    st.sidebar.header("Configuration")
    plant_names = db.df_static["plant_name"].tolist()
    selected_plant_name = st.sidebar.selectbox(
        "Select Plant", options=plant_names, key="cs_plant"
    )
    selected_plant_id = int(
        db.df_static.loc[
            db.df_static["plant_name"] == selected_plant_name, "plant_id"
        ].iloc[0]
    )
    end_date = st.sidebar.date_input("End Date", datetime.today().date(), key="cs_end")
    start_date = end_date - timedelta(days=30)

    st.sidebar.info(f"Data Window:\n**{start_date}** to **{end_date}**")

    if "current_plant" not in st.session_state:
        st.session_state["current_plant"] = selected_plant_name

    if st.session_state["current_plant"] != selected_plant_name:
        st.session_state.pop("df_cs_edited", None)
        st.session_state["current_plant"] = selected_plant_name

    if st.sidebar.button("Generate Clear Sky", type="primary", key="cs_gen_btn"):
        with st.spinner("Fetching data and generating profile..."):
            start_str = f"{start_date} 00:00:00+05:30"
            end_str = f"{end_date} 23:59:59+05:30"

            df_meas = db.get_meas_data(
                plant=selected_plant_name,
                start_date=start_str,
                end_date=end_str,
                source="raw",
            )

            if df_meas.empty:
                st.error("No data found for this window.")
            else:
                df_meas = df_meas.rename(columns={"record_time": "datetime"})
                df_meas["datetime"] = pd.to_datetime(df_meas["datetime"])
                df_meas["day_hour"] = (
                    df_meas["datetime"].dt.hour * 100 + df_meas["datetime"].dt.minute
                )
                df_meas["date"] = df_meas["datetime"].dt.date

                df_cs = (
                    df_meas.groupby("day_hour")[["active_power", "ghi"]]
                    .max()
                    .reset_index()
                )

                df_cs = df_cs.rename(
                    columns={
                        "active_power": "clearsky_active_power",
                        "ghi": "clearsky_ghi",
                    }
                )
                df_cs = df_cs.sort_values(by="day_hour", ascending=True)
                df_cs["plant_id"] = selected_plant_id
                df_cs = df_cs[
                    ["plant_id", "day_hour", "clearsky_active_power", "clearsky_ghi"]
                ]

                st.session_state["df_meas"] = df_meas
                st.session_state["df_cs_original"] = df_cs.copy(deep=True)
                st.session_state["df_cs_edited"] = df_cs.copy(deep=True)
                st.session_state["plant_id"] = selected_plant_id

    if "df_cs_edited" in st.session_state:
        df_meas = st.session_state["df_meas"]
        df_cs_orig = st.session_state["df_cs_original"]
        unique_dates = df_meas["date"].unique()

        st.divider()

        st.subheader("Edit Clear Sky Profile")
        st.info("Edit the values below. The graphs will update automatically.")

        edited_df = st.data_editor(
            st.session_state["df_cs_edited"],
            use_container_width=True,
            disabled=["plant_id", "day_hour"],
            key="data_editor",
        )
        st.session_state["df_cs_edited"] = edited_df

        if st.button("Save Edited Profile to Database", type="primary"):
            try:
                with db.conn.connect() as conn:
                    delete_query = text(
                        f"DELETE FROM re_insight.clearsky_table WHERE plant_id = {st.session_state['plant_id']}"
                    )
                    conn.execute(delete_query)
                    conn.commit()

                edited_df.to_sql(
                    "clearsky_table",
                    con=db.conn,
                    schema="re_insight",
                    if_exists="append",
                    index=False,
                )
                st.success(
                    f"Successfully updated database with {len(edited_df)} edited clear sky records!"
                )

            except Exception as e:
                st.error(f"Failed to update database: {e}")

        st.divider()

        st.subheader("GHI: 30-Day Observations vs Clear Sky")
        fig_ghi = go.Figure()

        for d in unique_dates:
            day_df = df_meas[df_meas["date"] == d].sort_values("day_hour")
            fig_ghi.add_trace(
                go.Scatter(
                    x=day_df["day_hour"],
                    y=day_df["ghi"],
                    mode="lines",
                    line=dict(color="darkgray", width=1.5),
                    opacity=0.6,
                    showlegend=False,
                    hoverinfo="skip",
                )
            )

        fig_ghi.add_trace(
            go.Scatter(
                x=df_cs_orig["day_hour"],
                y=df_cs_orig["clearsky_ghi"],
                mode="lines",
                name="Original Clear Sky",
                line=dict(color="#ff7f0e", width=2, dash="dash"),
            )
        )
        fig_ghi.add_trace(
            go.Scatter(
                x=edited_df["day_hour"],
                y=edited_df["clearsky_ghi"],
                mode="lines",
                name="Edited Clear Sky",
                line=dict(color="#9467bd", width=3),
            )
        )
        fig_ghi.update_layout(
            xaxis_title="Time of Day (day_hour)",
            yaxis_title="GHI (W/m²)",
            hovermode="x unified",
            margin=dict(l=0, r=0, t=30, b=0),
            legend=dict(
                orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1
            ),
        )
        st.plotly_chart(fig_ghi, use_container_width=True)

        st.subheader("Active Power: 30-Day Observations vs Clear Sky")
        fig_power = go.Figure()

        for d in unique_dates:
            day_df = df_meas[df_meas["date"] == d].sort_values("day_hour")
            fig_power.add_trace(
                go.Scatter(
                    x=day_df["day_hour"],
                    y=day_df["active_power"],
                    mode="lines",
                    line=dict(color="steelblue", width=1.5),
                    opacity=0.6,
                    showlegend=False,
                    hoverinfo="skip",
                )
            )

        fig_power.add_trace(
            go.Scatter(
                x=df_cs_orig["day_hour"],
                y=df_cs_orig["clearsky_active_power"],
                mode="lines",
                name="Original Clear Sky",
                line=dict(color="#2ca02c", width=2, dash="dash"),
            )
        )
        fig_power.add_trace(
            go.Scatter(
                x=edited_df["day_hour"],
                y=edited_df["clearsky_active_power"],
                mode="lines",
                name="Edited Clear Sky",
                line=dict(color="#d62728", width=3),
            )
        )
        fig_power.update_layout(
            xaxis_title="Time of Day (day_hour)",
            yaxis_title="Active Power (kW)",
            hovermode="x unified",
            margin=dict(l=0, r=0, t=30, b=0),
            legend=dict(
                orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1
            ),
        )
        st.plotly_chart(fig_power, use_container_width=True)


# This block ensures the app actually runs and stops the blank screen issue
if __name__ == "__main__":
    run_clearsky_app()
