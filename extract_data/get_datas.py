import sys
import os
from datetime import datetime
import pandas as pd
import json
from dotenv import load_dotenv

#Loading required files
current_folder = os.path.dirname(__file__)
from flight_data import LufthansaFly
from weather_data import Weather

load_dotenv()
datas_path = os.getenv("Datas_path")

filename_flight = "_flightdatas.parquet"
filename_weather = "_weatherdatas.parquet"

class GetDatas:
    def __init__(self, date: str):
        """
        date = 'AAAA-MM-DD'
        """
        self.date = date


    #Generate the files
    def get_flights(self, time: str):
        """
        time = 'HH:MM'
        """
        date_time = self.date + "T" + time
        self.df_flight_list = LufthansaFly(date_time).extract_flights()

        name_data_file = self.date + filename_flight
        file_path = os.path.join(datas_path, name_data_file)

        if os.path.exists(file_path):
            df_existant = pd.read_parquet(file_path)
            df_final = pd.concat([df_existant, self.df_flight_list], ignore_index=True)
            df_final.to_parquet(file_path, index=False)
            print(f"[INFO] Datas are added in the: {file_path} file !")

        else:
            self.df_flight_list.to_parquet(file_path, engine="pyarrow",  index=False)
            print(f"[INFO] Datas are available in the: {file_path} file !")

    def get_weather(self):
        self.df_weather = Weather(self.date).extract_weather()
        if self.df_weather is None:
            print("[WARNING] Can't generate weather file")
            return
            
        name_data_file = self.date + filename_weather
        file_path = os.path.join(datas_path, name_data_file)

        self.df_weather.to_parquet(file_path, engine="pyarrow",  index=False)
        print(f"[INFO] Datas are available in the: {file_path} file !")

    #def fused_datas(self):


    #Existing file verification
    #def save_datas(self):

if __name__ == "__main__":
    GetDatas("2026-04-04").get_flights("15:00")
    GetDatas("2026-04-04").get_weather()