import os
import yaml
import random
import time
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import dotenv
import pandas as pd

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

    @staticmethod
    def generate_sensor_data(min_val, max_val):
        """Generate random sensor data"""
        return random.randint(min_val, max_val)

    def generate_telemetry(self):
        sensors = self.config["telemetry-service"]["sensors"]
        telemetry = {"sensors": {}}
        for sensor, limits in sensors.items():
            value = self.generate_sensor_data(limits["min"], limits["max"])
            telemetry["sensors"][sensor] = {
                "value": value,
                "threshold_exceeded": value > limits["threshold"],
            }
        return telemetry

    def insert_telemetry(self, telemetry):
        """Upload telemetry to MongoDB"""
        db = self.client["telemetry_db"]
        collection = db["data"]
        collection.insert_one(telemetry)

    def process_csv_files(self):
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
                print(fname)
                file_path = os.path.join(input_dir, fname)

                try:
                    pd.read_csv(file_path)
                except Exception as e:
                    print(f"Error reading {file_path}: {e}")
                    try:
                        os.remove(file_path)
                    except Exception as e:
                        print(f"Error removing {file_path}: {e}")
                    continue

                packet_size = os.path.getsize(file_path)
                timestamp = os.path.splitext(fname)[0]
                output_file = os.path.join(output_dir, f"{timestamp}.csv")
                df_out = pd.DataFrame({"packet_size": [packet_size]})
                df_out.to_csv(output_file, index=False)

                try:
                    os.remove(file_path)
                except Exception as e:
                    print(f"Error removing {file_path}: {e}")

            print(f"Batch completed. Checking for more files...")

    def run(self):
        """Run telemetry service processing CSV files"""
        self.process_csv_files()


if __name__ == "__main__":
    service = TelemetryService()
    service.run()
