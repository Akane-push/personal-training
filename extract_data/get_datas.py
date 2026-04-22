import sys
import os
from datetime import datetime
import polars as pl
from dotenv import load_dotenv

load_dotenv()

filename_flight = "_flightdatas.parquet"
filename_weather = "_weatherdatas.parquet"

class GetDatas:
    def __init__(self, date: str):
        """
        date = 'AAAA-MM-DD'
        """
        self.date = date
        self.datas_path = self.service_check()

    #Change the path depending on the services
    def service_check(self):
        service = os.getenv("SERVICE_NAME", "unknown")

        if service == "airflow":
            from flight_data import LufthansaFly
            from weather_data import Weather
            self.LufthansaFly = LufthansaFly
            self.Weather = Weather
            return "/opt/airflow/output"
        
        else:
            #Loading required files
            current_folder = os.path.dirname(__file__)
            from flight_data import LufthansaFly
            from weather_data import Weather
            self.LufthansaFly = LufthansaFly
            self.Weather = Weather
            return os.getenv("Datas_path")


    #Generate the files for flight
    def get_flights(self, time: str):
        """
        time = 'HH:MM'
        """
        date_time = self.date + "T" + time
        self.df_flight_list = self.LufthansaFly(date_time).extract_flights()

        if self.df_flight_list.is_empty():
            print("[INFO] No available datas")
            return

        name_data_file = self.date + filename_flight
        file_path = os.path.join(self.datas_path, name_data_file)

        if os.path.exists(file_path):
            df_existant = pl.read_parquet(file_path)
            df_final = pl.concat([df_existant, self.df_flight_list], how="vertical")
            df_final.write_parquet(file_path)
            print(f"[INFO] Datas are added in the: {file_path} file !")

        else:
            self.df_flight_list.write_parquet(file_path)
            print(f"[INFO] Datas are available in the: {file_path} file !")

    #Generate the files for weather
    def get_weather(self):
        self.df_weather = self.Weather(self.date).extract_weather()
        if self.df_weather is None:
            print("[WARNING] Can't generate weather file")
            return
            
        name_data_file = self.date + filename_weather
        file_path = os.path.join(self.datas_path, name_data_file)

        self.df_weather.write_parquet(file_path)
        print(f"[INFO] Datas are available in the: {file_path} file !")
    

if __name__ == "__main__":
    GetDatas("2026-04-04").get_flights("15:00")
    GetDatas("2026-04-04").get_weather()