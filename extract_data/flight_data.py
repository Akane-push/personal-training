import sys
import os
import time
import requests
import polars as pl
import json

#Loading required files
current_folder = os.path.dirname(__file__)
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
        self.service_check()

        self.api = self.LufthansaAPI()
        if self.api.token is None:
            self.token = self.api.get_token()
        else:
            self.token = self.api.token
        self.headers = {'Authorization': f'Bearer {self.token}'}

    #Change the path depending on the services
    def service_check(self):
        service = os.getenv("SERVICE_NAME", "unknown")

        if service == "airflow":
            from identification import LufthansaAPI
            self.LufthansaAPI = LufthansaAPI

        else:
            sys.path.append(os.path.join(current_folder, "..", "tools"))
            from identification import LufthansaAPI
            self.LufthansaAPI = LufthansaAPI

    # Get landed flights informations
    def extract_flights(self):
        url = f"{self.api.url}/operations/customerflightinformation/arrivals"
        df_flight_list = pl.DataFrame()
        collected_dfs = []
        for airport in airports_list:
            data_json = requests.get(f"{url}/{airport}/{self.date}?limit=100", headers=self.headers).json().get('FlightInformation', {}).get('Flights', {}).get('Flight', [])
            filtered_json = self._flight_filter_generator(data_json)
            df_list = pl.from_dicts(self._row_iterator(filtered_json))

            print(airport)
            if not df_list.is_empty():
                collected_dfs.append(df_list)
        
            time.sleep(5)

        if collected_dfs:
            df_flight_list = pl.concat(collected_dfs)
        else:
            df_flight_list = pl.DataFrame()

        return df_flight_list
            
    def _flight_filter_generator(self, flights):
        for data in flights:
            if isinstance(data, dict) and data.get("Arrival", {}).get("Status", {}).get("Description") == "Flight Landed":
                yield data

    def _row_iterator(self, flights_gen):
        """Générateur : transforme chaque vol filtré en dictionnaire ligne par ligne."""
        for flight in flights_gen:
            op_carrier = flight.get('OperatingCarrier', {})
            dep_info = flight.get('Departure', {})
            arr_info = flight.get('Arrival', {})
            equip_info = flight.get('Equipment', {})
            
            flight_num = f"{op_carrier.get('AirlineID', '')}{op_carrier.get('FlightNumber', '')}"

            yield {
                'Flight_Number': flight_num,
                'Departure_IATA': dep_info.get("AirportCode"),
                'Dep_Scheduled_Date': dep_info.get("Scheduled", {}).get("Date"),
                'Dep_Scheduled_Time': dep_info.get("Scheduled", {}).get("Time"),
                'Dep_Actual_Date': dep_info.get("Actual", {}).get("Date"),
                'Dep_Actual_Time': dep_info.get("Actual", {}).get("Time"),
                'Arrival_IATA': arr_info.get("AirportCode"),
                'Arr_Scheduled_Date': arr_info.get("Scheduled", {}).get("Date"),
                'Arr_Scheduled_Time': arr_info.get("Scheduled", {}).get("Time"),
                'Arr_Actual_Date': arr_info.get("Actual", {}).get("Date"),
                'Arr_Actual_Time': arr_info.get("Actual", {}).get("Time"),
                'Aircraft_Code': equip_info.get('AircraftCode')
            }

if __name__ == "__main__":
    print(LufthansaFly().get_flights("2026-03-27T14:00"))