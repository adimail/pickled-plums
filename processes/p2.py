import os
import dotenv

dotenv.load_dotenv()
from azure.iot.device import IoTHubDeviceClient

INPUT_DIR = ""

def read_data(input_dir:str, device_client: any) -> str:
    data = None
    for fname in os.listdir(input_dir):
        if fname.endswith(".csv"):
            with open(fname, "r") as fh:
                data = fh.read()
            break
    try:
        device_client.send_message(data)
        os.remove(fname)
    except Exception as err:
        pass
                

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