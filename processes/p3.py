from datetime import datetime
import io
import os
import dotenv
import yaml
import ipdb
from azure.storage.blob import BlobServiceClient


dotenv.load_dotenv()


def load_config():
    if not os.path.exists("config.yaml"):
        raise FileNotFoundError("Missing config.yaml")

    with open("config.yaml") as f:
        config = yaml.safe_load(f)

    return config


config = load_config()
INPUT_DIR = config["input"]
BATCH_SIZE = config["blob"]["BATCH_SIZE"]
container_name = config["blob"]["container_name"]

conn_str = os.getenv("STORAGE_CONNECTION_STRING")
if conn_str:
    blob_service_client = BlobServiceClient.from_connection_string(conn_str)


container_client = blob_service_client.get_container_client(container=container_name)


def _ingest():
    current_csvs = [fl for fl in os.listdir(INPUT_DIR) if fl.endswith(".csv")]
    if len(current_csvs) < BATCH_SIZE:
        return None
    final_rows = []
    for fl in current_csvs:
        with open(os.path.join(INPUT_DIR, fl), "r") as fh:
            cur_data = fh.read().split("\n")
            if len(final_rows) == 0:
                final_rows = cur_data
            else:
                final_rows += cur_data[1:]
    return final_rows, current_csvs


def process(data, csvs):
    ipdb.set_trace()
    dt = datetime.now()
    ts = str(dt.timestamp()).split(".")[0]
    try:
        with io.BytesIO(bytes("\n".join(data), "utf-8")) as fh:
            container_client.upload_blob(
                name=f"{dt.year}/{dt.month}/{dt.day}/{dt.hour}/{ts}.csv",
                data=fh,
                overwrite=True,
            )
    except Exception as err:
        print(err)
    for fl in csvs:
        os.remove(os.path.join(INPUT_DIR, fl))


def main():
    result = _ingest()
    if result is not None:
        data, csvs = result
        process(data, csvs)


main()
