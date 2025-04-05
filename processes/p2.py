import os
import dotenv
import yaml

dotenv.load_dotenv()
from azure.iot.device import IoTHubDeviceClient


def load_config():
    if not os.path.exists("config.yaml"):
        raise FileNotFoundError("Missing config.yaml")

    with open("config.yaml") as f:
        config = yaml.safe_load(f)

    input_path = os.path.join(config["simulation"]["input"])
    print(input_path)
    return input_path


INPUT_DIR = load_config()


def read_data(input_dir, device_client):
    """Process and send messages from CSV files in the input directory."""
    for fname in os.listdir(input_dir):
        ip = os.path.join(input_dir, fname)
        if os.path.isfile(ip) and fname.endswith(".csv"):
            with open(ip, "r") as fh:
                data = fh.read()
            try:
                device_client.send_message(data)
                os.remove(ip)
                print(ip)
            except Exception as err:
                print("Exception:", err)


def main():
    conn_str = os.getenv("IOTHUB_DEVICE_CONNECTION_STRING")
    if not conn_str:
        raise ValueError("IOTHUB_DEVICE_CONNECTION_STRING environment variable not set")
    device_client = IoTHubDeviceClient.create_from_connection_string(conn_str)
    device_client.connect()
    try:
        while True:
            try:
                read_data(input_dir=INPUT_DIR, device_client=device_client)
            except Exception:
                pass
    except KeyboardInterrupt:
        pass
    finally:
        device_client.shutdown()


if __name__ == "__main__":
    main()
