import openmeteo_requests
from datetime import date, timedelta
import pandas as pd
import requests_cache
from retry_requests import retry


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
			print(f"\nCoordinates: {response.Latitude()}°N {response.Longitude()}°E")
			print(f"Elevation: {response.Elevation()} m asl")
			print(f"Timezone difference to GMT+0: {response.UtcOffsetSeconds()}s")
			print(f"Model Nº: {response.Model()}")
			
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

models = ["ecmwf_ifs025", "gfs_global", "icon_global", "gem_global", "ukmo_global_deterministic_10km"]
df = get_openmeteo_control_data(24.69, 69.24,end_date='2026-05-23', apikey = "x", models=models)
print(df)