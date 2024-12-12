import os
from dotenv import load_dotenv
from telethon import TelegramClient, functions
import asyncio
import pytz
import json
import logging
from tqdm import tqdm
from datetime import date, datetime
import re
from gspread.exceptions import WorksheetNotFound
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import json
import os.path

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

SHEET_CONFIGS = {
    "channels_daily": {
        "key_columns": ["channel_id", "date"],
        "merge_columns": ["channel_name", "member_count", "messages_count"],
        "timestamp_column": "processed_at",
    },
    "channel_messages": {
        "key_columns": ["channel_id", "message_id"],
        "merge_columns": ["text", "processed_text", "date"],
        "timestamp_column": "processed_at",
    },
    "chat_topics_hourly": {
        "key_columns": ["chat_id", "topic_id", "hour"],
        "merge_columns": ["chat_name", "topic_name", "message_count"],
        "timestamp_column": "processed_at",
    },
}


class SheetStorage:
    def __init__(self, credentials_path, spreadsheet_url):
        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive",
        ]
        creds = ServiceAccountCredentials.from_json_keyfile_name(
            credentials_path, scope
        )
        self.client = gspread.authorize(creds)
        self.spreadsheet = self.client.open_by_url(spreadsheet_url)
        self.logger = logging.getLogger(__name__)

    def _get_or_create_sheet(self, name):
        try:
            return self.spreadsheet.worksheet(name)
        except WorksheetNotFound:
            return self.spreadsheet.add_worksheet(name, 1000, 26)

    def merge_data(self, sheet_name, new_data, config):
        self.logger.info(f"Starting merge for sheet: '{sheet_name}' ...")
        sheet = self._get_or_create_sheet(sheet_name)

        # Convert to DataFrames
        existing_data = pd.DataFrame(sheet.get_all_records())
        new_df = pd.DataFrame(new_data)

        # Convert datetime columns
        for col in new_df.columns:
            if pd.api.types.is_datetime64_any_dtype(new_df[col]) or isinstance(
                new_df[col].iloc[0], (datetime, date)
            ):
                new_df[col] = pd.to_datetime(new_df[col]).dt.strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
                if not existing_data.empty and col in existing_data.columns:
                    existing_data[col] = pd.to_datetime(existing_data[col]).dt.strftime(
                        "%Y-%m-%d %H:%M:%S"
                    )

        if not existing_data.empty:
            # Update existing records and add new ones
            merged = pd.concat([existing_data, new_df]).drop_duplicates(
                subset=config["key_columns"], keep="last"
            )
        else:
            merged = new_df

        sheet.clear()
        sheet.update([merged.columns.values.tolist()] + merged.values.tolist())
        self.logger.info(f"Successfully updated '{sheet_name}' \n")


def save_cache(data, filename):
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, default=datetime_handler, ensure_ascii=False, fp=f)


def load_cache(filename):
    if os.path.exists(filename):
        with open(filename, "r", encoding="utf-8") as f:
            return json.load(f)
    return None


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
        if self.mode == "backfill":
            self.start_date = datetime.strptime(os.getenv("START_DATE"), "%Y-%m-%d")
            self.end_date = datetime.strptime(os.getenv("END_DATE"), "%Y-%m-%d")


def clean_text(text):
    """Clean text for word cloud"""
    text = re.sub(r"http\S+|www\S+|https\S+", "", text, flags=re.MULTILINE)
    text = re.sub(r"[,.\n:#]", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip().lower()


def datetime_handler(obj):
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")


def mask_channel_link(link):
    """Mask parts of channel link for privacy"""
    if not link:
        return link
    # If it's a private channel link with a hash
    if "+" in link:
        base, hash_part = link.split("+")
        return f"{base}+{'*' * (len(hash_part) // 2)}{hash_part[len(hash_part) // 2:]}"
    # For public channels
    parts = link.split("/")
    if len(parts) > 1:
        channel_name = parts[-1]
        return f"{parts[0]}//{parts[2]}/{'*' * (len(channel_name) // 2)}{channel_name[len(channel_name) // 2:]}"
    return link


async def get_chat_stats(client, chat_id, timezone):
    """Get stats for a forum chat including topics and user activity"""
    try:
        masked_id = mask_channel_link(chat_id)
        logger.info(f"Processing chat: {masked_id} ...")
        chat = await client.get_entity(chat_id)
        stats = {
            "chat_id": masked_id,
            "chat_name": chat.title,
            "timestamp": datetime.now(timezone),
            "topics": {},
            "total_messages": 0,
        }

        # Get total messages
        async for message in client.iter_messages(chat, limit=1):
            stats["total_messages"] = message.id

        try:
            # Get forum topics
            result = await client(
                functions.channels.GetForumTopicsRequest(
                    channel=chat, offset_date=0, offset_id=0, offset_topic=0, limit=100
                )
            )

            logger.info(f"Processing {len(result.topics)} topics ...")
            for topic in tqdm(result.topics, desc="Processing topics"):
                topic_id = topic.id
                topic_messages = await client.get_messages(
                    chat, limit=0, reply_to=topic_id
                )

                stats["topics"][topic_id] = {
                    "message_count": topic_messages.total,
                    "title": topic.title,
                    "top_id": topic.id,
                }
                await asyncio.sleep(1)

        except Exception as e:
            logger.error(f"Error getting topics for {masked_id}: {e}")
            stats["topics_error"] = str(e)

        return stats
    except Exception as e:
        logger.error(f"Error getting chat stats for {masked_id}: {e}")
        return None


async def get_channel_stats(client, channel_id, timezone):
    """Get channel stats and messages for word cloud"""
    try:
        masked_id = mask_channel_link(channel_id)
        channel = await client.get_entity(channel_id)
        logger.info(f"Processing channel: {channel.title} ...")
        stats = {
            "channel_id": masked_id,
            "channel_name": channel.title,
            "timestamp": datetime.now(timezone),
            "messages": [],
            "member_count": 0,
        }

        participants = await client.get_participants(channel, limit=0)
        stats["member_count"] = participants.total

        # Get recent messages
        messages = []
        message_count = 0

        async for message in client.iter_messages(channel, limit=100):
            if message.text:
                messages.append(
                    {
                        "date": message.date.astimezone(timezone),
                        "text": message.text,
                        "processed_text": clean_text(message.text),
                        "message_id": message.id,
                    }
                )
            message_count += 1

        stats["messages"] = messages
        return stats
    except Exception as e:
        logger.error(f"Error getting channel stats for {masked_id}: {e}")
        return None


async def get_channel_names(client, channel_list):
    names = {}
    for channel_id in channel_list:
        try:
            entity = await client.get_entity(channel_id)
            names[channel_id] = entity.title
        except Exception as e:
            logger.error(f"Error getting name for {mask_channel_link(channel_id)}: {e}")
            names[channel_id] = channel_id
    return names


async def print_welcome_msg(config):
    # Get channel names first
    logger.info("Collecting channel and chat names...")
    async with TelegramClient("anon", config.api_id, config.api_hash) as client:
        channel_names = await get_channel_names(client, config.channels["channels"])
        chat_names = await get_channel_names(client, config.channels["chats"])

    # Welcome message
    print("\nThanks for using rzv_de private telegram stats bot!")
    print("\nProcessing channels:")
    for name in channel_names.values():
        print(f"- {name}")
    print("\nProcessing chats:")
    for name in chat_names.values():
        print(f"- {name}")

    print("\nCurrent config (safe to print part):")
    safe_config = {"timezone": config.timezone.zone, "mode": config.mode}
    if config.mode == "backfill":
        safe_config.update(
            {
                "start_date": config.start_date.strftime("%Y-%m-%d"),
                "end_date": config.end_date.strftime("%Y-%m-%d"),
            }
        )
    print(json.dumps(safe_config, indent=2))

    print("\nHave a nice day!\n")


async def main():
    config = Config()

    await print_welcome_msg(config)

    cached_data = load_cache(config.cache_file)

    if cached_data:
        logger.info("Loading from cache")
        all_stats = cached_data
    else:
        logger.info("Collecting fresh data")
        async with TelegramClient("anon", config.api_id, config.api_hash) as client:
            all_stats = {
                "channels": [],
                "chats": [],
                "timestamp": datetime.now(pytz.UTC),
            }

            # Process channels with progress bar
            for channel_id in tqdm(
                config.channels["channels"], desc="Processing channels"
            ):
                await asyncio.sleep(2)
                stats = await get_channel_stats(client, channel_id, config.timezone)
                if stats:
                    all_stats["channels"].append(stats)

            # Process chats with progress bar
            for chat_id in tqdm(config.channels["chats"], desc="Processing chats"):
                await asyncio.sleep(2)
                stats = await get_chat_stats(client, chat_id, config.timezone)
                if stats:
                    all_stats["chats"].append(stats)

            logger.info("Data collection completed!\n")
            save_cache(all_stats, config.cache_file)
            # Use the custom datetime handler for JSON serialization
            logger.debug(
                json.dumps(
                    all_stats, default=datetime_handler, ensure_ascii=False, indent=2
                )
            )

    storage = SheetStorage(config.credentials_path, config.sheet_url)

    # Store channels data
    channels_daily = [
        {
            "channel_id": c["channel_id"],
            "channel_name": c["channel_name"],
            "date": datetime.now(config.timezone).date(),
            "member_count": c["member_count"],
            "messages_count": len(c["messages"]),
            "processed_at": datetime.now(config.timezone),
        }
        for c in all_stats["channels"]
    ]

    storage.merge_data(
        "channels_daily", channels_daily, SHEET_CONFIGS["channels_daily"]
    )

    # Store messages
    messages = []
    for channel in all_stats["channels"]:
        for msg in channel["messages"]:
            messages.append(
                {
                    "channel_id": channel["channel_id"],
                    "message_id": msg["message_id"],
                    "text": msg["text"],
                    "processed_text": msg["processed_text"],
                    "date": msg["date"],
                    "processed_at": datetime.now(config.timezone),
                }
            )

    storage.merge_data("channel_messages", messages, SHEET_CONFIGS["channel_messages"])

    # Store chat topics hourly data
    chat_topics = []
    for chat in all_stats["chats"]:
        for topic_id, topic_data in chat["topics"].items():
            chat_topics.append(
                {
                    "chat_id": chat["chat_id"],
                    "chat_name": chat["chat_name"],
                    "topic_id": topic_id,
                    "topic_name": topic_data["title"],
                    "hour": datetime.now(config.timezone).replace(
                        minute=0, second=0, microsecond=0
                    ),
                    "message_count": topic_data["message_count"],
                    "processed_at": datetime.now(config.timezone),
                }
            )

    storage.merge_data(
        "chat_topics_hourly", chat_topics, SHEET_CONFIGS["chat_topics_hourly"]
    )

    if os.path.exists(config.cache_file):
        os.remove(config.cache_file)
        print(" ")
        logger.info("Cache cleared")


if __name__ == "__main__":
    asyncio.run(main())
