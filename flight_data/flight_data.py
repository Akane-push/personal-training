import sys
import os
import time
import requests
import pandas as pd
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "id"))
from identification import LufthansaAPI

datas_path = os.getenv("Datas_path")
limit_call_per_hour = 1000
airports_list = ["CDG", "FRA"]

class LufthansaFly:
    def __init__(self):
        self.api = LufthansaAPI()
        if self.api.token is None:
            self.token = self.api.get_token()
        else:
            self.token = self.api.token
        self.headers = {'Authorization': f'Bearer {self.token}'}

    # Get landed flights informations
    def get_flights(self):
        url = f"{self.api.url}/operations/customerflightinformation/arrivals"
        df_flight_list = pd.DataFrame()
        for airport in airports_list:
            data_json = requests.get(f"{url}/{airport}/2026-03-28T14:00?limit=20", headers=self.headers).json().get('FlightInformation', {}).get('Flights', {}).get('Flight', [])
            filtered_json = [data for data in data_json if data["Arrival"]["Status"]["Description"] == "Flight Landed"]
            df_list = pd.DataFrame({
                    'Flight_Number': [f"{flight['OperatingCarrier']['AirlineID']}{flight['OperatingCarrier']['FlightNumber']}" for flight in filtered_json],
                    'Departure_IATA': [code["Departure"]["AirportCode"] for code in filtered_json],
                    'Dep_Scheduled_Date': [date["Departure"]["Scheduled"]["Date"] for date in filtered_json],
                    'Dep_Scheduled_Time': [time["Departure"]["Scheduled"]["Time"] for time in filtered_json],
                    'Dep_Actual_Date': [date["Departure"]["Actual"]["Date"] for date in filtered_json],
                    'Dep_Actual_Time': [time["Departure"]["Actual"]["Time"] for time in filtered_json],
                    'Arrival_IATA': [code["Arrival"]["AirportCode"] for code in filtered_json],
                    'Arr_Scheduled_Date': [date["Arrival"]["Scheduled"]["Date"] for date in filtered_json],
                    'Arr_Scheduled_Time': [time["Arrival"]["Scheduled"]["Time"] for time in filtered_json],
                    'Arr_Actual_Date': [date["Arrival"]["Actual"]["Date"] for date in filtered_json],
                    'Arr_Actual_Time': [time["Arrival"]["Actual"]["Time"] for time in filtered_json],
                    'Aircraft_Code': [code['Equipment']['AircraftCode'] for code in filtered_json]
                                    })
            print(df_list)
            df_flight_list = pd.concat([df_flight_list, df_list], axis = 0)
            time.sleep(6)
        return df_flight_list

    def get_datas(self):
        df_flight_list = self.get_flights()

        name_data_file = "datas.parquet"
        file_path = os.path.join(datas_path, name_data_file)
        #os.makedirs(os.path.dirname(file_path), exist_ok=True)

        df_flight_list.to_parquet(file_path, engine="pyarrow",  index=False)
        print(f"[INFO] Datas are available in the: {file_path} file !")


if __name__ == "__main__":
    LufthansaFly().get_datas()