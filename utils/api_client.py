import requests
import pandas as pd
from io import StringIO


class APIClient:
    BASE_URL = "https://api.openf1.org/v1/"

    def fetch_data(self, key, session_key=None, driver_number=None):
        params = {"csv": "true"}
        if session_key is not None:
            params["session_key"] = session_key
        if driver_number is not None:
            params["driver_number"] = driver_number

        response = requests.get(f"{self.BASE_URL}{key}", params=params)
        if response.status_code == 200:
            df = pd.read_csv(StringIO(response.text))
            return df
        else:
            print(f"Failed to fetch data for key: {key}")
            return None
