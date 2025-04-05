import time
import requests
import yaml
import csv
import os
import argparse


def load_config(process_name):
    with open("config.yaml") as f:
        config = yaml.safe_load(f)
    latency = config["simulation"]["processes"][process_name]["latency"]
    output_folder = config["simulation"]["output"]
    return latency, output_folder


def ensure_output_dir(path):
    if not os.path.exists(path):
        os.makedirs(path)


def write_to_csv(data, directory):
    timestamp = str(int(time.time()))
    filename = os.path.join(directory, timestamp + ".csv")
    with open(filename, mode="w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["Name", "Value", "TS"])
        for sensor in data["data"]["sensors"]:
            writer.writerow([sensor["Name"], sensor["Value"], sensor["TS"]])


def run(process_name):
    latency, base_output = load_config(process_name)
    output_path = os.path.join(base_output, process_name)
    ensure_output_dir(output_path)
    while True:
        try:
            res = requests.get("http://localhost:8080/sensors", timeout=3)
            if res.status_code == 200:
                data = res.json()
                write_to_csv(data, output_path)
            else:
                print(
                    "[{}] API returned status code {}".format(
                        process_name, res.status_code
                    )
                )
        except requests.exceptions.RequestException as e:
            print("[{}] Request error: {}".format(process_name, str(e)))
        except Exception as e:
            print("[{}] General error: {}".format(process_name, str(e)))
        time.sleep(latency)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--process", required=True)
    args = parser.parse_args()
    run(args.process)
