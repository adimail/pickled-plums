import os
import dotenv
import yaml
import ipdb

dotenv.load_dotenv()
from azure.iot.device import IoTHubDeviceClient


def load_config(process_name):
    if not os.path.exists("config.yaml"):
        raise FileNotFoundError("Missing config.yaml")

    with open("config.yaml") as f:
        config = yaml.safe_load(f)

    input_path = os.path.join(config["simulation"]["input"])
    print(input_path)
    return input_path


INPUT_DIR = load_config("p2")


def read_data(input_dir: str, device_client: any) -> str:
    data = None
    for fname in os.listdir(input_dir):
        print(fname)
        if fname.endswith(".csv"):
            with open(os.path.join(input_dir, fname), "r") as fh:
                data = fh.read()
            break
    try:
        print("[LOG] Inside try")
        device_client.send_message(data)
        os.remove(fname)
    except Exception as err:
        print("Exception err", err)


def main():
    conn_str = os.getenv("IOTHUB_DEVICE_CONNECTION_STRING")
    device_client = IoTHubDeviceClient.create_from_connection_string(conn_str)
    device_client.connect()
    while True:
        try:
            read_data(input_dir=INPUT_DIR, device_client=device_client)
        except Exception as err:
            pass
    device_client.shutdown()


main()
