import logging
from data.database import engine, SessionLocal
from data.models import create_dynamic_model, metadata
from utils.api_client import APIClient
from sqlalchemy.dialects.postgresql import insert
import numpy as np

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
        # Mapping of primary keys for each table
        self.primary_keys_mapping = {
            "meetings": ["meeting_key"],
            "pit": [
                "session_key",
                "driver_number",
            ],
            "position": [
                "session_key",
                "driver_number",
            ],
            "race_control": [
                "session_key",
            ],
            "sessions": ["session_key"],
            "stints": [
                "session_key",
                "driver_number",
            ],
            "team_radio": [
                "session_key",
                "driver_number",
            ],
            "weather": [
                "session_key",
            ],
            "drivers": ["driver_number"],
            "laps": [
                "session_key",
                "driver_number",
            ],
            "intervals": [
                "session_key",
                "driver_number",
            ],
            "location": [
                "session_key",
                "driver_number",
            ],
            "car_data": [
                "session_key",
                "driver_number",
            ],
        }
        logging.info("DataLoader initialized.")

    def load_data(self):
        # Load data for the first set of keys
        for key in self.keys:
            logging.info(f"Processing {key}")
            df = self.api_client.fetch_data(key)
            if df is not None and not df.empty:
                primary_keys = self.primary_keys_mapping.get(key)
                self.store_data(key, df, primary_keys)
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
                        # Add session_key and driver_number to df if not present
                        if "session_key" not in df.columns:
                            df["session_key"] = session_key
                        if "driver_number" not in df.columns:
                            df["driver_number"] = driver_number
                        primary_keys = self.primary_keys_mapping.get(key)
                        self.store_data(key, df, primary_keys)
                    else:
                        logging.warning(
                            f"No data returned for {key} with session {session_key} and driver {driver_number}"
                        )

    def store_data(self, key, df, primary_keys=None):
        # Use the key as the table name
        table_name = key

        logging.info(f"Preparing to store data in table {table_name}")

        # Create dynamic model
        table = create_dynamic_model(table_name, df, primary_keys)
        metadata.create_all(engine, tables=[table])

        try:
            with engine.begin() as conn:
                if primary_keys:
                    # Check for nulls in primary keys
                    if df[primary_keys].isnull().any().any():
                        logging.error(
                            f"Null values found in primary key columns for table {table_name}"
                        )
                        logging.error(df[df[primary_keys].isnull().any(axis=1)])
                        logging.error("Skipping the rows with null primary key values")
                        df.dropna(subset=primary_keys, how="any", inplace=True)
                    df.replace(
                        {np.nan: None, np.inf: None, -np.inf: None}, inplace=True
                    )
                    records = df.to_dict(orient="records")
                    stmt = insert(table).values(records)
                    stmt = stmt.on_conflict_do_nothing(index_elements=primary_keys)
                    result = conn.execute(stmt)
                    logging.info(f"Inserted {result.rowcount} rows into {table_name}")
                else:
                    # Use df.to_sql with the connection from the context manager
                    df.to_sql(
                        table_name,
                        con=conn,
                        if_exists="append",
                        index=False,
                        method="multi",
                        dtype=None,  # Let SQLAlchemy infer the data types
                    )
                    logging.info(
                        f"Data stored in table {table_name} without primary keys"
                    )
        except Exception as e:
            logging.exception(f"Failed to insert data into {table_name}: {e}")


if __name__ == "__main__":
    data_loader = DataLoader()
    data_loader.load_data()
