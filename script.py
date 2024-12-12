import os
from dotenv import load_dotenv
from telethon import TelegramClient, functions, errors
from telethon.tl.types import InputMessagesFilterEmpty
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
        "key_columns": ["channel_id", "message_id", "word"],
        "merge_columns": ["date"],
        "timestamp_column": "processed_at",
    },
    "chat_topics_hourly": {
        "key_columns": ["chat_id", "topic_id", "hour"],
        "merge_columns": [
            "chat_name",
            "topic_name",
            "message_count",
            "first_message_id",
            "last_message_id",
        ],
        "timestamp_column": "processed_at",
    },
}


class DateFilter(InputMessagesFilterEmpty):
    def __init__(self, start_date, end_date):
        self.start_date = start_date
        self.end_date = end_date

    def filter(self, message):
        return self.start_date <= message.date <= self.end_date


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

    def get_date_range(self):
        if self.mode == "backfill":
            return self.start_date, self.end_date
        return None, None


def clean_text(text):
    """Clean text for word cloud"""
    # First remove URLs
    text = re.sub(r"http\S+|www\S+|https\S+", "", text, flags=re.MULTILINE)
    # Remove all special chars, including those at word boundaries
    text = re.sub(r"[-*?()\"'\+;\.\,:`<>\#\[\]%\(\)]+|[?!]+$", " ", text)
    # Normalize spaces
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


async def get_messages_by_hour(
    client, chat, topic_id, timezone, start_date=None, end_date=None
):
    messages_by_hour = {}
    total_messages = 0
    message_filter = DateFilter(start_date, end_date) if start_date else None

    logger.info(f"Starting messages collection for topic {topic_id}")
    async for message in client.iter_messages(
        chat, reply_to=topic_id, reverse=True, filter=message_filter
    ):
        total_messages += 1
        if total_messages % 1000 == 0:
            logger.info(f"Processed {total_messages} messages for topic {topic_id}")

        msg_date = message.date.astimezone(timezone)
        hour = msg_date.replace(minute=0, second=0, microsecond=0)
        hour_str = hour.strftime("%Y-%m-%dT%H:%M:%S")

        if hour_str not in messages_by_hour:
            messages_by_hour[hour_str] = {
                "count": 0,
                "first_id": message.id,
                "last_id": message.id,
                "hour": hour,
            }
        current = messages_by_hour[hour_str]
        current["count"] += 1
        current["last_id"] = max(current["last_id"], message.id)
        current["first_id"] = min(current["first_id"], message.id)

    logger.info(f"Completed topic {topic_id} with {total_messages} messages")
    return messages_by_hour


async def get_chat_stats(client, chat_id, timezone, start_date=None, end_date=None):
    max_retries = 3
    for retry in range(max_retries):
        try:
            masked_id = mask_channel_link(chat_id)
            chat = await client.get_entity(chat_id)
            logger.info(f"Processing chat: {chat.title} ...")
            stats = {
                "chat_id": masked_id,
                "chat_name": chat.title,
                "timestamp": datetime.now(timezone),
                "topics": {},
            }

            try:
                result = await client(
                    functions.channels.GetForumTopicsRequest(
                        channel=chat,
                        offset_date=0,
                        offset_id=0,
                        offset_topic=0,
                        limit=100,
                    )
                )

                for topic in tqdm(result.topics, desc="Processing topics"):
                    messages = await get_messages_by_hour(
                        client, chat, topic.id, timezone, start_date, end_date
                    )
                    stats["topics"][topic.id] = {
                        "title": topic.title,
                        "messages": messages,
                    }
                    await asyncio.sleep(1)

                return stats

            except Exception as e:
                logger.error(f"Error getting topics for {masked_id}: {e}")
                stats["topics_error"] = str(e)
                return None

        except errors.FloodWaitError as e:
            if retry == max_retries - 1:
                raise
            await asyncio.sleep(e.seconds)
        except Exception as e:
            logger.error(f"Error getting chat stats for {masked_id}: {e}")
            return None


async def get_channel_stats(client, channel_id, timezone):
    """Get channel stats and messages for word cloud"""
    max_retries = 3
    for retry in range(max_retries):
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
                            "date": message.date.astimezone(timezone).strftime(
                                "%Y-%m-%dT%H:%M:%S"
                            ),
                            "text": message.text,
                            "processed_text": clean_text(message.text),
                            "message_id": message.id,
                        }
                    )
                message_count += 1

            stats["messages"] = messages
            return stats
        except errors.FloodWaitError as e:
            if retry == max_retries - 1:
                raise
            await asyncio.sleep(e.seconds)
            continue
        except Exception as e:
            logger.error(f"Error getting channel stats for {masked_id}: {e}")
            return None


async def get_channel_names(client, channel_list):
    names = {}
    for channel_id in channel_list:
        max_retries = 3
        for retry in range(max_retries):
            try:
                entity = await client.get_entity(channel_id)
                names[channel_id] = entity.title
            except errors.FloodWaitError as e:
                if retry == max_retries - 1:
                    raise
                await asyncio.sleep(e.seconds)
                continue
            except Exception as e:
                logger.error(
                    f"Error getting name for {mask_channel_link(channel_id)}: {e}"
                )
                names[channel_id] = mask_channel_link(channel_id)
    return names


async def print_welcome_msg(config):
    try:
        logger.info("Collecting channel and chat names...")
        async with TelegramClient("anon", config.api_id, config.api_hash) as client:
            # Add timeouts
            channel_names = await asyncio.wait_for(
                get_channel_names(client, config.channels["channels"]), timeout=30
            )
            chat_names = await asyncio.wait_for(
                get_channel_names(client, config.channels["chats"]), timeout=30
            )

        print("\nrzv_de telegram stats bot")
        print("\nChannels:")
        for name in channel_names.values():
            print(f"- {name}")
        print("\nChats:")
        for name in chat_names.values():
            print(f"- {name}")

        safe_config = {"timezone": config.timezone.zone, "mode": config.mode}
        if config.mode == "backfill":
            safe_config.update(
                {
                    "start_date": config.start_date.strftime("%Y-%m-%d"),
                    "end_date": config.end_date.strftime("%Y-%m-%d"),
                }
            )
        print("\nConfig:", json.dumps(safe_config, indent=2))

    except asyncio.TimeoutError:
        logger.error("Timeout collecting names")
    except Exception as e:
        logger.error(f"Error in welcome message: {e}")


async def main():
    config = Config()
    PROCESSED_AT = datetime.now(config.timezone)
    start_date, end_date = config.get_date_range()

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
                await asyncio.sleep(5)
                stats = await get_channel_stats(client, channel_id, config.timezone)
                if stats:
                    all_stats["channels"].append(stats)

            # Process chats with progress bar
            for chat_id in tqdm(config.channels["chats"], desc="Processing chats"):
                await asyncio.sleep(2)
                stats = await get_chat_stats(
                    client, chat_id, config.timezone, start_date, end_date
                )
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
            "date": PROCESSED_AT.date(),
            "member_count": c["member_count"],
            "messages_count": len(c["messages"]),
            "processed_at": PROCESSED_AT,
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
            if msg["processed_text"]:
                for word in msg["processed_text"].split():
                    messages.append(
                        {
                            "channel_id": channel["channel_id"],
                            "message_id": msg["message_id"],
                            "word": word,
                            "date": datetime.fromisoformat(msg["date"]).strftime(
                                "%Y-%m-%dT%H:%M:%S"
                            ),
                            "processed_at": PROCESSED_AT,
                        }
                    )

    storage.merge_data("channel_messages", messages, SHEET_CONFIGS["channel_messages"])

    # Store chat topics hourly data
    chat_topics = []
    for chat in all_stats["chats"]:
        for topic_id, topic_data in chat["topics"].items():
            for hour_str, message_data in topic_data["messages"].items():
                parsed_hour = datetime.fromisoformat(hour_str).strftime(
                    "%Y-%m-%dT%H:%M:%S"
                )
                chat_topics.append(
                    {
                        "chat_id": chat["chat_id"],
                        "chat_name": chat["chat_name"],
                        "topic_id": topic_id,
                        "topic_name": topic_data["title"],
                        "hour": parsed_hour,
                        "message_count": message_data["count"],
                        "first_message_id": message_data["first_id"],
                        "last_message_id": message_data["last_id"],
                        "processed_at": PROCESSED_AT,
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
