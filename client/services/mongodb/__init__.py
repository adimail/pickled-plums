from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import dotenv
import os


dotenv.load_dotenv()
uri = os.getenv("mongo_db_uri")
print(uri)

client = MongoClient(uri, server_api=ServerApi("1"))

try:
    client.admin.command("ping")
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)
