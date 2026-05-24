import streamlit as st
import plotly.graph_objects as go
import openmeteo_requests
from datetime import date, timedelta
import pandas as pd
import requests_cache
from retry_requests import retry
from sklearn.metrics import mean_absolute_error, root_mean_squared_error as rmse
import os


def fetch_sgis_data(start, end):
    date_series = pd.date_range(start=start, end=end, freq='D')
    data_list = []
    for date in date_series:
        fname = f"D:/work/weather/sgis/monitor_data/{date.strftime('PSS1_%Y%m%d.csv')}"
        if os.path.exists(fname):
            df = pd.read_csv(fname, 
            usecols=['DateTime', 'GHI'])
            data_list.append(df)
    if len(data_list) == 0:
        return pd.DataFrame()
    df_out =  pd.concat(data_list)
    df_out["DateTime"] = pd.to_datetime(df_out["DateTime"])
    df_out["DateTime"] = df_out["DateTime"].dt.tz_localize(tz = None)
    return df_out

def get_openmeteo_control_data(lat, lon, start_date  = "", end_date = "", apikey=None, models=["ecmwf_ifs025"], tz = "Asia/Kolkata"):
	# Setup the Open-Meteo API client with cache and retry on error
	cache_session = requests_cache.CachedSession('.cache', expire_after = 3600)
	retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
	openmeteo = openmeteo_requests.Client(session = retry_session)

	# Make sure all required weather variables are listed here
	# The order of variables in hourly or daily is important to assign them correctly below
	url = "https://customer-api.open-meteo.com/v1/forecast"
	if start_date == "":
		start_date = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
	if end_date == "":
		end_date = (date.today() + timedelta(days=2)).strftime("%Y-%m-%d")
	df_final = pd.DataFrame()
	for i, imodel in enumerate(models):
		params = {
			"latitude": lat,
			"longitude": lon,
			"models": imodel,
			"minutely_15": "shortwave_radiation",
			"start_date": start_date,
			"end_date": end_date,
			"timezone": tz,
			"apikey": apikey,
		}
		responses = openmeteo.weather_api(url, params = params)

		# Process 1 location and 5 models
		for response in responses:
			# print(f"\nCoordinates: {response.Latitude()}°N {response.Longitude()}°E")
			# print(f"Elevation: {response.Elevation()} m asl")
			# print(f"Timezone difference to GMT+0: {response.UtcOffsetSeconds()}s")
			# print(f"Model Nº: {response.Model()}")
			
			# Process minutely_15 data. The order of variables needs to be the same as requested.
			minutely_15 = response.Minutely15()
			minutely_15_shortwave_radiation = minutely_15.Variables(0).ValuesAsNumpy()
			
			minutely_15_data = {
				"DateTime": pd.date_range(
					start = pd.to_datetime(minutely_15.Time(), unit = "s", utc = True),
					end =  pd.to_datetime(minutely_15.TimeEnd(), unit = "s", utc = True),
					freq = pd.Timedelta(seconds = minutely_15.Interval()),
					inclusive = "left"
				)
			}
			
			minutely_15_data[imodel] = minutely_15_shortwave_radiation
			
			minutely_15_dataframe = pd.DataFrame(data = minutely_15_data)
			if i == 0:
				df_final = minutely_15_dataframe
			else:
				df_final = pd.merge(df_final, minutely_15_dataframe, on = "DateTime", how = "outer")
	if len(df_final) > 0:
		df_final["DateTime"] = df_final["DateTime"].dt.tz_convert("Asia/Kolkata")
	return df_final


def sample_page():
    st.header("☀️ Solar Dayahead dashboard")
    date_start = st.date_input(
            "Select Start Date", 
            value=pd.Timestamp('today').date() + pd.Timedelta('1D'),
            max_value=pd.Timestamp('today').date() + pd.Timedelta('1D'),
            min_value=pd.Timestamp('today').date() - pd.Timedelta(days = 7)
        )
    date_end = st.date_input(
            "Select End Date", 
            value=pd.Timestamp('today').date() + pd.Timedelta('1D'),
            max_value=pd.Timestamp('today').date() + pd.Timedelta('1D'),
            min_value=pd.Timestamp('today').date() - pd.Timedelta(days = 7)
        )
    history_date = date.today() - timedelta(days = 1)
    forecast_date = date.today() + timedelta(days = 1)

    selected_models = st.multiselect(
        "Select Models:",
        options=["ecmwf_ifs025", "gfs_global", "icon_global", "gem_global", "ukmo_global_deterministic_10km"],
        default=["ecmwf_ifs025", "gfs_global", "icon_global", "gem_global", "ukmo_global_deterministic_10km"],
    )
    if date_end <= history_date:
        history_date = date_end

    df_sgis = fetch_sgis_data(date_start, history_date)
    df_opnemeteo = get_openmeteo_control_data(24.69, 63.24, date_start, date_end, apikey="rjlUQOn5yR5RbGPH", models=selected_models)
    df_opnemeteo["DateTime"] = df_opnemeteo["DateTime"].dt.tz_localize(tz = None)
    

    if len(df_sgis) == 0:
        st.write("No measured data for selected period")
        df_final = df_opnemeteo.copy()
        df_final["GHI"] = float('nan')
    else:
        df_final = pd.merge(df_sgis, df_opnemeteo, on = "DateTime", how = "outer")
    
    st.set_page_config(page_title="Solar Dayahead dashboard", layout="wide")

    st.title("Model vs Actual comparison")

    # 3. Build the Plotly Figure
    fig = go.Figure()

    colors = {"GHI": "#FFA500", "ecmwf_ifs025": "#FF4B4B", "icon_global": "#1F77B4", "gfs_global": "#FFA500", "gem_global": "#FF4B4B", "ukmo_global_deterministic_10km": "#1F77B4"}  # Orange, Red, Blue
    dash_styles = {"GHI": "solid", "ecmwf_ifs025": "dash", "icon_global": "dash", "gfs_global": "dash", "gem_global": "dash", "ukmo_global_deterministic_10km": "dash"}
    columns = selected_models
    columns.append("GHI")
    # if selected_models:
    for metric in columns:
        fig.add_trace(
            go.Scatter(
                x=df_final["DateTime"],
                y=df_final[metric],
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
    df_score = pd.DataFrame(columns=selected_models)
    df_final_nonan = df_final.dropna(subset=["GHI"])
    if len(df_final_nonan) > 0:
        for imodel in selected_models:
            df_score.loc["MAE", imodel] = mean_absolute_error(df_final_nonan["GHI"], df_final_nonan[imodel])
            df_score.loc["RMSE", imodel] = rmse(df_final_nonan["GHI"], df_final_nonan[imodel])
        
    df_fct = df_final[df_final['DateTime'].dt.date >= forecast_date]
    df_fct = df_fct.dropna(how = 'all', axis = 1)
    # 4. Render the interactive chart in Streamlit
    st.plotly_chart(fig, use_container_width=True)
    st.title("Model Scores")
    st.dataframe(df_score)
    st.title("Raw forecast data")
    st.dataframe(df_fct)

pages = {"Solar Dayahead Forecasting": sample_page}

st.sidebar.title("Navigation")
page = st.sidebar.radio("Go to", pages.keys())

pages[page]()