import openmeteo_requests

from openmeteo_sdk.Variable import Variable
from openmeteo_sdk.Aggregation import Aggregation

import pandas as pd
import requests_cache
from retry_requests import retry

# Setup the Open-Meteo API client with cache and retry on error
cache_session = requests_cache.CachedSession('.cache', expire_after = 3600)
retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
openmeteo = openmeteo_requests.Client(session = retry_session)

# Make sure all required weather variables are listed here
# The order of variables in hourly or daily is important to assign them correctly below
url = "https://customer-ensemble-api.open-meteo.com/v1/ensemble"
params = {
	"latitude": 52.52,
	"longitude": 13.41,
	"hourly": "shortwave_radiation",
	"models": ["ncep_gefs025", "ukmo_global_ensemble_20km", "icon_global_eps", "ecmwf_ifs025_ensemble", "gem_global_ensemble", "bom_access_global_ensemble"],
	"start_date": "2026-05-15",
	"end_date": "2026-05-29",
	"apikey": "rjlUQOn5yR5RbGPH",
}
responses = openmeteo.weather_api(url, params = params)

# Process 1 location and 6 models
for response in responses:
	print(f"\nCoordinates: {response.Latitude()}°N {response.Longitude()}°E")
	print(f"Elevation: {response.Elevation()} m asl")
	print(f"Timezone difference to GMT+0: {response.UtcOffsetSeconds()}s")
	print(f"Model Nº: {response.Model()}")
	
	# Process hourly data. The order of variables needs to be the same as requested.
	hourly = response.Hourly()
    hourly_variables = [hourly.Variables(i) for i in range(hourly.VariablesLength())]

	hourly_shortwave_radiation = filter(lambda x: x.Variable() == Variable.shortwave_radiation and x.Altitude() == 2, hourly_variables)	
	
	hourly_data = {
		"date": pd.date_range(
			start = pd.to_datetime(hourly.Time(), unit = "s", utc = True),
			end =  pd.to_datetime(hourly.TimeEnd(), unit = "s", utc = True),
			freq = pd.Timedelta(seconds = hourly.Interval()),
			inclusive = "left"
		)
	}
	
	# Process all hourly members
	for variable in hourly_shortwave_radiation:
		member = variable.EnsembleMember()
		hourly_data[f"shortwave_radiation_member{member}"] = variable.ValuesAsNumpy()
	
	hourly_dataframe = pd.DataFrame(data = hourly_data)
	print("\nHourly data\n", hourly_dataframe)
	