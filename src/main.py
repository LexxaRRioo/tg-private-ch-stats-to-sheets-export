import asyncio
import logging
import json
import os
import pytz
from pathlib import Path
from datetime import datetime
from tqdm import tqdm
from telethon import TelegramClient
from telethon.sessions import StringSession
from src.config import Config
from src.telegram.client import get_channel_stats, get_chat_stats, get_channel_names
from src.sheets.client import SheetStorage
from src.sheets.config import SHEET_CONFIGS
from src.cache import load_cache, save_cache, datetime_handler
from src.telegram.utils import mask_channel_link

ROOT_DIR = Path(__file__).parent.parent
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    force=True
)

# Modify the logger to use tqdm.write
class TqdmLoggingHandler(logging.Handler):
    def emit(self, record):
        try:
            msg = self.format(record)
            tqdm.write(msg)
            self.flush()
        except Exception:
            self.handleError(record)

# Set up the logger
logger = logging.getLogger()
logger.handlers = []  # Remove existing handlers
logger.addHandler(TqdmLoggingHandler())

async def print_welcome_msg(config):
    try:
        logger.info("Collecting channel and chat names...")
        async with TelegramClient(
            StringSession(os.getenv("TG_SESSION")), config.api_id, config.api_hash
        ) as client:
            channel_names = await asyncio.wait_for(
                get_channel_names(client, config.channels["channels"]), timeout=30
            )
            chat_names = await asyncio.wait_for(
                get_channel_names(client, config.channels["chats"]), timeout=30
            )

        print("\nWelcome to the rzv_de telegram stats bot")
        print("\nChannels:")
        for name in channel_names.values():
            print(f"- {name}")
        print("\nChats:")
        for name in chat_names.values():
            print(f"- {name}")

        safe_config = {"timezone": config.timezone.zone, "mode": config.mode}
        print("\nConfig:", json.dumps(safe_config, indent=2, default=datetime_handler, ensure_ascii=False))

    except asyncio.TimeoutError:
        logger.error("Timeout collecting names")
    except Exception as e:
        logger.error(f"Error in welcome message: {e}")


async def main():
    config = Config()
    cache_path = os.path.join(ROOT_DIR, config.cache_file)
    PROCESSED_AT = datetime.now(config.timezone)

    await print_welcome_msg(config)
    cached_data = load_cache(cache_path)

    if cached_data:
        logger.info("Loading from cache")
        all_stats = cached_data
    else:
        logger.info("Collecting fresh data")
        async with TelegramClient(
            StringSession(os.getenv("TG_SESSION")), config.api_id, config.api_hash
        ) as client:
            all_stats = {
                "channels": [],
                "chats": [],
                "timestamp": datetime.now(pytz.UTC),
            }

            channel_progress = tqdm(
                config.channels["channels"],
                desc="Starting",
                position=0,
                leave=True,
                bar_format="Processing channel '{desc}': {bar} | {percentage:3.0f}% | {n_fmt}/{total_fmt}",
                ncols=100,
            )

            for channel_id in channel_progress:
                await asyncio.sleep(5)
                stats = await get_channel_stats(client, channel_id, config.timezone)
                if stats:
                    channel_progress.set_description(stats["channel_name"])
                    all_stats["channels"].append(stats)

            chat_progress = tqdm(
                config.channels["chats"],
                desc="Processing chats",
                position=0,  # Main progress bar at top
                ncols=80,
                leave=True,
                bar_format="{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt}",
            )

            for chat_id in chat_progress:
                await asyncio.sleep(2)
                stats = await get_chat_stats(client, chat_id, config.timezone)
                if stats:
                    all_stats["chats"].append(stats)

            logger.info("Data collection completed!\n")
            save_cache(all_stats, cache_path)

    storage = SheetStorage(config.credentials_path, config.sheet_url)

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

    hashtags_data = []
    for channel in all_stats["channels"]:
        for occurrence in channel["hashtag_occurrences"]:
            hashtags_data.append({
                "channel_id": channel["channel_id"],
                "channel_name": channel["channel_name"],
                "message_id": occurrence["message_id"],
                "hashtag": occurrence["hashtag"],
                "date": occurrence["date"],
                "processed_at": PROCESSED_AT
            })

    if hashtags_data:
        storage.merge_data(
            "hashtags_detailed", 
            hashtags_data, 
            SHEET_CONFIGS["hashtags_detailed"]
        )

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

    if os.path.exists(cache_path):
        os.remove(cache_path)
        logger.info("Cache cleared")


if __name__ == "__main__":
    asyncio.run(main())
