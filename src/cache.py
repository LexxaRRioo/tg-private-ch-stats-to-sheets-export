import json
import os
from datetime import date, datetime
from pathlib import Path

ROOT_DIR = Path(__file__).parent.parent

def datetime_handler(obj):
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

def save_cache(data, filename):
    filepath = os.path.join(ROOT_DIR, filename) if not os.path.isabs(filename) else filename
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, default=datetime_handler, ensure_ascii=False, fp=f)

def load_cache(filename):
    filepath = os.path.join(ROOT_DIR, filename) if not os.path.isabs(filename) else filename
    if os.path.exists(filepath):
        with open(filepath, "r", encoding="utf-8") as f:
            return json.load(f)
    return None