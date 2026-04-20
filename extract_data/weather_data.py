import openmeteo_requests
import polars as pl
import sys
import os
from dotenv import load_dotenv
import requests_cache
from retry_requests import retry
from datetime import datetime, timedelta

#Loading required files
current_folder = os.path.dirname(__file__)
path = os.path.join(current_folder, "..", "reference_data", "airports_references.parquet")
df_IATA_refs = pl.read_parquet(path)
load_dotenv()
filename_flight = "_flightdatas.parquet"

#Open-Meteo config
cache_session = requests_cache.CachedSession('.cache', expire_after = -1)
retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
openmeteo = openmeteo_requests.Client(session = retry_session)

class Weather:
    def __init__(self, date: str):
        """
        date = 'AAAA-MM-DDTHH:MM' (Convert to ISO 8601 format with 'T' separator)
        """
        self.date = date
        self.datas_path = self.service_check()

    #Change the path depending on the services
    def service_check(self):
        service = os.getenv("SERVICE_NAME", "unknown")

        if service == "airflow":
            return "/opt/airflow/output"
        
        else:
            return os.getenv("Datas_path")

    #Load Datas from Open-Meteo API
    def extract_weather(self):
        if self.loading_datas() is None:
            return None

        url = "https://archive-api.open-meteo.com/v1/archive"
        params = {
            "latitude": self.df_IATA_l_L['Latitude'].tolist(),
            "longitude": self.df_IATA_l_L['Longitude'].tolist(),
            "start_date": (datetime.strptime(self.date, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d"),
            "end_date": self.date,
            "hourly": ["temperature_2m", "wind_speed_100m", "wind_direction_100m", "surface_pressure", "weather_code", "precipitation", "wind_gusts_10m", "wind_direction_10m", "wind_speed_10m", "cloud_cover", "cloud_cover_low", "cloud_cover_mid", "cloud_cover_high"],
            "timezone": "auto",
        }
        self.responses = openmeteo.weather_api(url, params = params)
        
        return self.openmeteo_extract()

    #Load flight data file and extract IATA list, longitude and latitude
    def loading_datas(self):
        file_name = self.date + filename_flight
        file_path = os.path.join(self.datas_path, file_name)

        if os.path.exists(file_path):
            self.df_flight_list = pl.read_parquet(file_path)
            df_IATA = pl.concat([self.df_flight_list['Departure_IATA'], self.df_flight_list['Arrival_IATA']])
            self.liste_IATA = df_IATA.drop_duplicates().tolist()
        
        else:
            print(f"[WARNING] No {file_name} file existing")
            return None

        self.df_IATA_l_L = df_IATA_refs[df_IATA_refs.index.isin(self.liste_IATA)]
        self.df_IATA_l_L = self.df_IATA_l_L.drop(self.df_IATA_l_L.columns[[0, 1, 2]], axis=1)
        return self.df_IATA_l_L

    #Transform Open-Meteo data into a data frame
    def openmeteo_extract(self):
        self.hourly_df = pl.DataFrame()
        for i in range(len(self.responses)):
            response = self.responses[i]
            hourly = response.Hourly()
            hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()
            hourly_wind_speed_100m = hourly.Variables(1).ValuesAsNumpy()
            hourly_wind_direction_100m = hourly.Variables(2).ValuesAsNumpy()
            hourly_surface_pressure = hourly.Variables(3).ValuesAsNumpy()
            hourly_weather_code = hourly.Variables(4).ValuesAsNumpy()
            hourly_precipitation = hourly.Variables(5).ValuesAsNumpy()

            hourly_data = {"date": pl.date_range(
                start = pl.to_datetime(hourly.Time() + response.UtcOffsetSeconds(), unit = "s", utc = True),
                end =  pl.to_datetime(hourly.TimeEnd() + response.UtcOffsetSeconds(), unit = "s", utc = True),
                freq = pl.Timedelta(seconds = hourly.Interval()),
                inclusive = "left"
            )}

            hourly_data["IATA"] = self.liste_IATA[i]
            hourly_data["Temperature_2m"] = hourly_temperature_2m
            hourly_data["Wind_Speed_100m"] = hourly_wind_speed_100m
            hourly_data["Wind_Direction_100m"] = hourly_wind_direction_100m
            hourly_data["Surface_Pressure"] = hourly_surface_pressure
            hourly_data["Weather_Code"] = hourly_weather_code
            hourly_data["Precipitation"] = hourly_precipitation
            
            hourly_data = pl.DataFrame(data = hourly_data)
            dates_list = hourly_data["date"].dt.date.tolist()
            hours_list = hourly_data["date"].dt.strftime("%H:%M").tolist()
            hourly_data = hourly_data.drop("date", axis=1)
            hourly_data.insert(1, 'Date', dates_list)
            hourly_data.insert(2, 'Time', hours_list)

            self.hourly_df = pl.concat([self.hourly_df, hourly_data])
        return self.hourly_df

if __name__ == "__main__":
    print(Weather("2026-03-30").get_weather().head())