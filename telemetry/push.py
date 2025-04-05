import os
import yaml
import time
import dotenv
import pandas as pd
from datetime import datetime
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

dotenv.load_dotenv()


class TelemetryService:
    def __init__(self):
        self.config = self.load_config()
        self.client = self.init_mongo_client()
        self.BATCH_SIZE = self.config["blob"]["BATCH_SIZE"]

    @staticmethod
    def load_config():
        """Load configuration from config.yaml"""
        if not os.path.exists("config.yaml"):
            raise FileNotFoundError("Missing config.yaml")
        with open("config.yaml") as f:
            return yaml.safe_load(f)

    @staticmethod
    def init_mongo_client():
        """Initialize MongoDB client"""
        uri = os.getenv("mongo_db_uri")
        client = MongoClient(uri, server_api=ServerApi("1"))
        try:
            client.admin.command("ping")
            print("Pinged your deployment. You successfully connected to MongoDB!")
        except Exception as e:
            print(e)
            raise e
        return client

    def insert_telemetry(self, telemetry):
        """Upload telemetry to MongoDB"""
        db = self.client["bitminimal"]
        collection = db["telemetry"]
        collection.insert_one(telemetry)

    def process_csv_files(self):
        """Process CSV files and save telemetry data to file and MongoDB"""
        input_dir = os.path.join(self.config.get("output", "."), "csv")
        output_dir = os.path.join(self.config.get("output", "."), "telemetry")
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        while True:
            csv_files = [f for f in os.listdir(input_dir) if f.endswith(".csv")]
            total_files = len(csv_files)
            if total_files < self.BATCH_SIZE:
                print(
                    f"Waiting for more files. Current: {total_files}, Required: {self.BATCH_SIZE}"
                )
                time.sleep(5)
                continue
            batch_files = csv_files[: self.BATCH_SIZE]
            for fname in batch_files:
                file_path = os.path.join(input_dir, fname)
                base_name = os.path.splitext(fname)[0]
                parts = base_name.split("-")
                if len(parts) >= 3:
                    date_str = "-".join(parts[:3])
                else:
                    date_str = base_name
                try:
                    df = pd.read_csv(file_path)
                    row_data = df.set_index("Name")["Value"].to_dict()
                    row_data["Time"] = fname.split(".")[0]
                    row_data["Time"] = datetime.fromtimestamp(int(row_data["Time"]))
                    self.insert_telemetry(row_data)
                    row_df = pd.DataFrame([row_data])
                    daily_file = os.path.join(output_dir, f"{date_str}.csv")
                    if os.path.exists(daily_file):
                        existing_df = pd.read_csv(daily_file)
                        combined_df = pd.concat(
                            [existing_df, row_df], ignore_index=True
                        )
                    else:
                        combined_df = row_df
                    combined_df.to_csv(daily_file, index=False)
                    os.remove(file_path)
                except Exception as e:
                    print(f"Error processing {file_path}: {e}")
                    try:
                        os.remove(file_path)
                    except:
                        pass
            print("Batch completed. Checking for more files...")

    def run(self):
        """Run telemetry service processing CSV files"""
        self.process_csv_files()


if __name__ == "__main__":
    service = TelemetryService()
    service.run()
