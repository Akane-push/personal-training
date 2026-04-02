import sys
import os
from datetime import datetime
import pandas as pd
import json
from dotenv import load_dotenv

#Loading required files
current_folder = os.path.dirname(__file__)
from flight_data import LufthansaFly
#from weather_data import weather_data

load_dotenv()
datas_path = os.getenv("Datas_path")

class GetDatas:
    def __init__(self, date: str):
        """
        date = 'AAAA-MM-DDTHH:MM' (Convert to ISO 8601 format with 'T' separator)
        """
        self.date = date
        self.df_flight_list = LufthansaFly().get_flights(self.date)


    #Generate the files
    def get_datas(self):
        name_data_file = f"{self.date.split('T')[0]}_flydatas.parquet"
        file_path = os.path.join(datas_path, name_data_file)

        if os.path.exists(file_path):
            df_existant = pd.read_parquet(file_path)
            df_final = pd.concat([df_existant, self.df_flight_list], ignore_index=True)
            df_final.to_parquet(file_path, index=False)
            print(f"[INFO] Datas are added in the: {file_path} file !")

        else:
            self.df_flight_list.to_parquet(file_path, engine="pyarrow",  index=False)
            print(f"[INFO] Datas are available in the: {file_path} file !")

    #Existing file verification
    #def save_datas(self):

if __name__ == "__main__":
    GetDatas("2026-03-29T14:00").get_datas()