import logging
from data.database import engine, SessionLocal
from data.models import create_dynamic_model, metadata
from utils.api_client import APIClient
import pandas as pd

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class DataLoader:
    def __init__(self):
        self.api_client = APIClient()
        self.session = SessionLocal()
        self.keys = [
            "meetings",
            "pit",
            "position",
            "race_control",
            "sessions",
            "stints",
            "team_radio",
            "weather",
            "drivers",
        ]
        self.keys_2 = [
            "laps",
            "intervals",
            "location",
            "car_data",
        ]
        logging.info("DataLoader initialized.")

    def load_data(self):
        # Load data for the first set of keys
        for key in self.keys:
            logging.info(f"Processing {key}")
            df = self.api_client.fetch_data(key)
            if df is not None and not df.empty:
                self.store_data(key, df)
            else:
                logging.warning(f"No data returned for {key}")

        # Get unique session_keys and driver_numbers
        sessions_df = self.api_client.fetch_data("sessions")
        drivers_df = self.api_client.fetch_data("drivers")

        if sessions_df is None or drivers_df is None:
            logging.error("Failed to fetch sessions or drivers data.")
            return

        session_keys = sessions_df["session_key"].unique()
        driver_numbers = drivers_df["driver_number"].unique()

        # Load data for the second set of keys
        for key in self.keys_2:
            logging.info(f"Processing {key}")
            for session_key in session_keys:
                for driver_number in driver_numbers:
                    logging.info(
                        f"Fetching {key} for session {session_key} and driver {driver_number}"
                    )
                    df = self.api_client.fetch_data(key, session_key, driver_number)
                    if df is not None and not df.empty:
                        self.store_data(f"{key}_{session_key}_{driver_number}", df)
                    else:
                        logging.warning(
                            f"No data returned for {key} with session {session_key} and driver {driver_number}"
                        )

    def store_data(self, table_name, df):
        # Create dynamic model
        table = create_dynamic_model(table_name, df)
        metadata.create_all(engine, tables=[table])

        # Insert data
        conn = engine.connect()
        try:
            df.to_sql(
                table_name, con=conn, if_exists="append", index=False, method="multi"
            )
            logging.info(f"Data stored in table {table_name}")
        except Exception as e:
            logging.error(f"Failed to insert data into {table_name}: {e}")
        finally:
            conn.close()


if __name__ == "__main__":
    data_loader = DataLoader()
    data_loader.load_data()
