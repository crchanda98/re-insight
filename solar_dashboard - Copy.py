import streamlit as st
import pandas as pd
import plotly.graph_objects as go

def fetch_sgis_data(start, end):
    date_series = pd.date_range(start=start, end=end, freq='D')
    data_list = []
    for date in date_series:
        df = pd.read_csv(f"D:/work/weather/sgis/monitor_data/{date.strftime('PSS1_%Y%m%d.csv')}")
        data_list.append(df)
    df_out =  pd.concat(data_list)
    df_out["DateTime"] = pd.to_datetime(df_out["DateTime"])
    df_out["DateTime"] = df_out["DateTime"].dt.tz_localize(tz = None)
    return df_out
    
def sample_page():
    st.header("Solar DA")
    st.write("This is a sample page")
    date_start = st.date_input(
            "Select Start Date", 
            value=pd.Timestamp('today').date() - pd.Timedelta('2D'),
            max_value=pd.Timestamp('today').date() - pd.Timedelta('2D'),
            min_value=pd.Timestamp('today').date() - pd.Timedelta(days = 7)
        )
    date_end = st.date_input(
            "Select End Date", 
            value=pd.Timestamp('today').date() - pd.Timedelta('2D'),
            max_value=pd.Timestamp('today').date() - pd.Timedelta('2D'),
            min_value=pd.Timestamp('today').date() - pd.Timedelta(days = 7)
        )

    df_sgis = fetch_sgis_data(date_start, date_end)
    st.set_page_config(page_title="Solar Irradiance Dashboard", layout="wide")

    st.title("☀️ Interactive Solar Irradiance Viewer")
    st.markdown("Hover, zoom, and pan over the interactive Plotly chart below.")


    # 2. Setup Streamlit sidebar controls
    # st.sidebar.subheader("Filter Options")
    selected_metrics = st.multiselect(
        "Select Irradiance Components:",
        options=["GHI", "DNI", "DIF"],
        default=["GHI", "DNI", "DIF"],
    )

    # 3. Build the Plotly Figure
    fig = go.Figure()

    colors = {"GHI": "#FFA500", "DNI": "#FF4B4B", "DIF": "#1F77B4"}  # Orange, Red, Blue
    dash_styles = {"GHI": "solid", "DNI": "dash", "DIF": "dashdot"}

    if selected_metrics:
        for metric in selected_metrics:
            fig.add_trace(
                go.Scatter(
                    x=df_sgis["DateTime"],
                    y=df_sgis[metric],
                    name=metric,
                    mode="lines+markers",  # Added markers since your current data points are all 0
                    line=dict(color=colors[metric], width=2, dash=dash_styles[metric]),
                    hovertemplate=f"<b>{metric}</b><br>Time: %{{x}}<br>Value: %{{y}} W/m²<extra></extra>",
                )
            )

        # Update chart styling and axes
        fig.update_layout(
            title="Solar Irradiance Components over Time",
            xaxis_title="Date/Time",
            yaxis_title="Irradiance (W/m²)",
            hovermode="x unified",  # Shows all active metrics in one hover box
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            margin=dict(l=40, r=40, t=80, b=40),
            template="plotly_white",
        )

        # 4. Render the interactive chart in Streamlit
        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(df_sgis)

pages = {"Sample": sample_page}

st.sidebar.title("Navigation")
page = st.sidebar.radio("Go to", pages.keys())

pages[page]()