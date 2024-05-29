import os
import yaml
from dotenv import load_dotenv
load_dotenv()

with open("config.yaml", encoding = "utf-8") as f:
    config = yaml.safe_load(f)

request_authorization = os.getenv("iMarine_database_authorization")