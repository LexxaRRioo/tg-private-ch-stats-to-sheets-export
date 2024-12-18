import os
from dotenv import load_dotenv
import json
import pytz


class Config:
    def __init__(self):
        load_dotenv()
        self.api_id = int(os.getenv("TELEGRAM_API_ID"))
        self.api_hash = os.getenv("TELEGRAM_API_HASH")
        self.sheet_url = os.getenv("GOOGLE_SHEET_URL")
        self.credentials_path = os.getenv("GOOGLE_CREDENTIALS_PATH")
        channels_json = os.getenv("TELEGRAM_CHANNELS")
        self.channels = json.loads(channels_json)
        self.timezone = pytz.timezone(os.getenv("TIMEZONE", "Europe/Moscow"))
        self.mode = os.getenv("MODE", "regular")
        self.cache_file = "data_cache.json"
