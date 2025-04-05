from datetime import datetime
import io
import os
from azure.storage.blob import BlobServiceClient, ContainerClient, BlobBlock, BlobClient, StandardBlobTier

INPUT_DIR = ""
BATCH_SIZE = 100
account_url = ""
blob_service_client = BlobServiceClient.from_connection_string(
    conn_str="")
container_name = ""



container_client = blob_service_client.get_container_client(container=container_name)
with open("sample.txt", mode="rb") as data:
    blob_client = container_client.upload_blob(name="/bark/sample.txt", data=data, overwrite=True)


def _ingest() -> any:
    # ingest data
    current_csvs = [fl for fl in os.listdir(INPUT_DIR) if fl.endswith(".csv")]
    if len(current_csvs) < 100:
        return None
    final_rows = []
    for fl in current_csvs:
        with open(os.path.join(INPUT_DIR, fl), "r") as fh:
            cur_data = fh.read().split("\n")
            if len(final_rows) == 0:
                final_rows = cur_data
            else:
                final_rows += cur_data[1:]
    return final_rows



def process(data: any):
    dt = datetime.now()
    ts = str(dt.timestamp()).split(".")[0]
    try:
        with io.BytesIO(bytes("\n".join(data), "utf-8")) as fh:
            container_client.upload_blob(
                name=f"{dt.year}/{dt.month}/{dt.day}/{dt.hour}/{ts}.csv",
                data=fh,
                overwrite=True)
    except Exception as err:
        print(err)
    for fl in data:
        os.remove(os.path.join(INPUT_DIR, fl))


def main():
    process(_ingest())
main()