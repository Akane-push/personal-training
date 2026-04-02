import openmeteo_requests
import pandas as pd
import sys
import os
from dotenv import load_dotenv
import requests_cache
from retry_requests import retry
from datetime import datetime, timedelta

#Loading required files
current_folder = os.path.dirname(__file__)
df_IATA_refs = pd.read_parquet(os.path.join(current_folder, "..", "reference_data", "airports_references.parquet"))
load_dotenv()
datas_path = os.getenv("Datas_path")

#Open-Meteo config
cache_session = requests_cache.CachedSession('.cache', expire_after = -1)
retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
openmeteo = openmeteo_requests.Client(session = retry_session)

class Weather:
    def __init__(self, date: str):
        """
        date = 'AAAA-MM-DD'
        """
        self.date = date

        self.df_IATA_longitudelatitude = self.loading_datas()
        if self.df_IATA_longitudelatitude is None:
            return


        '''url = "https://archive-api.open-meteo.com/v1/archive"
        params = {
            "latitude": 52.52,
            "longitude": 13.41,
            "start_date": (datetime.strptime(self.date, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d"),
            "end_date": self.date,
            "hourly": ["temperature_2m", "wind_speed_100m", "wind_direction_100m", "surface_pressure", "weather_code", "precipitation", "wind_gusts_10m", "wind_direction_10m", "wind_speed_10m", "cloud_cover", "cloud_cover_low", "cloud_cover_mid", "cloud_cover_high"],
            "timezone": "auto",
        }
        self.responses = openmeteo.weather_api(url, params = params)'''

    def loading_datas(self):
        file_name = f"{self.date}_flydatas.parquet"
        file_path = os.path.join(datas_path, file_name)

        if os.path.exists(file_path):
            self.df_flight_list = pd.read_parquet(file_path)
            df_IATA = pd.concat([self.df_flight_list['Departure_IATA'], self.df_flight_list['Arrival_IATA']], ignore_index=True)
            liste_IATA = df_IATA.drop_duplicates().tolist()
        
        else:
            print(f"[WARNING] No {self.date}_flydatas.parquet file existing")
            return None
        print(liste_IATA)
        df_ = df_IATA_refs[df_IATA_refs.index.isin(liste_IATA)]
        print(df_.head())

        iATA_latitude = df_IATA_refs.loc[liste_IATA, 'Latitude'].tolist()
        iATA_longitude = df_IATA_refs.loc[liste_IATA, 'Longitude'].tolist()
        print(iATA_latitude, iATA_longitude)
        #return df_IATA

    
    #def get_datas(self):



if __name__ == "__main__":
    df=Weather("2026-03-30")
    #print(df.head())