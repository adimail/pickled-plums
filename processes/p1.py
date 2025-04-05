import time
import requests
import yaml
import csv
import os
import argparse
import logging

logger = logging.getLogger("process_logger")
logger.setLevel(logging.INFO)
formatter = logging.Formatter(
    "[%(asctime)s] [%(levelname)s] %(message)s", datefmt="%H:%M:%S"
)
ch = logging.StreamHandler()
ch.setFormatter(formatter)
logger.addHandler(ch)


def load_config(process_name):
    """Load configuration for a given process from config.yaml and return latency and output path."""
    if not os.path.exists("config.yaml"):
        raise FileNotFoundError("Missing config.yaml")

    with open("config.yaml") as f:
        config = yaml.safe_load(f)

    latency = config["simulation"]["processes"][process_name]["latency"]
    output_path = os.path.join(config["output"], "csv")
    return latency, output_path


def write_to_csv(data, directory):
    """Write sensor data to a CSV file with a timestamp as its name."""
    os.makedirs(directory, exist_ok=True)
    ts = data["data"]["TS"]
    filename = os.path.join(directory, f"{ts}.csv")

    with open(filename, mode="w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["Name", "Value"])
        for sensor in data["data"]["sensors"]:
            writer.writerow([sensor["Name"], sensor["Value"]])


def run(process_name):
    """Run the process: load config, create output and log directories, fetch sensor data and log actions."""
    latency, output_path = load_config(process_name)

    os.makedirs(output_path, exist_ok=True)
    os.makedirs("logs", exist_ok=True)

    log_file = os.path.join("logs", f"{process_name}.log")
    fh = logging.FileHandler(log_file)
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    logger.info("Starting process %s", process_name)

    while True:
        try:
            res = requests.get("http://localhost:8080/sensors", timeout=3)
            if res.status_code == 200:
                write_to_csv(res.json(), output_path)
                logger.info("Sensor data written to %s", output_path)
            else:
                logger.error("API returned status code %s", res.status_code)
        except requests.exceptions.RequestException as e:
            logger.error("Request error: %s", e)
        except Exception as e:
            logger.error("General error: %s", e)

        time.sleep(latency)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--process", required=True)
    args = parser.parse_args()
    run(args.process)
