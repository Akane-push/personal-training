import sys
import os
import time
import requests
import pandas as pd
import json

#Loading required files
current_folder = os.path.dirname(__file__)
sys.path.append(os.path.join(current_folder, "..", "id"))
from identification import LufthansaAPI
airports_json = os.path.join(current_folder, "..", "reference_data", "study_airport.json")
with open(airports_json, 'r', encoding='utf-8') as f:
    airports_list = json.load(f)

limit_call_per_hour = 1000


class LufthansaFly:
    #Identification
    def __init__(self, date: str):
        """
        date = 'AAAA-MM-DDTHH:MM' (Convert to ISO 8601 format with 'T' separator)
        """
        self.date = date

        self.api = LufthansaAPI()
        if self.api.token is None:
            self.token = self.api.get_token()
        else:
            self.token = self.api.token
        self.headers = {'Authorization': f'Bearer {self.token}'}

    # Get landed flights informations
    def extract_flights(self):
        url = f"{self.api.url}/operations/customerflightinformation/arrivals"
        df_flight_list = pd.DataFrame()
        for airport in airports_list:
            data_json = requests.get(f"{url}/{airport}/{self.date}?limit=100", headers=self.headers).json().get('FlightInformation', {}).get('Flights', {}).get('Flight', [])
            filtered_json = [data for data in data_json if isinstance(data, dict) and data.get("Arrival", {}).get("Status", {}).get("Description") == "Flight Landed"]
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
            #print(df_list)
            df_flight_list = pd.concat([df_flight_list, df_list], axis = 0)
            time.sleep(5)
        return df_flight_list

if __name__ == "__main__":
    print(LufthansaFly().get_flights("2026-03-27T14:00"))