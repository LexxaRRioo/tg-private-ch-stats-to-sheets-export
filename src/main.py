import asyncio
import logging
import json
import os
import pytz
from pathlib import Path
from datetime import datetime, timedelta
from tqdm import tqdm
from telethon import TelegramClient
from telethon.sessions import StringSession
from src.config import Config
from src.telegram.client import get_channel_stats, get_chat_stats, get_channel_names
from src.telegram.utils import mask_channel_link
from src.sheets.client import SheetStorage
from src.sheets.config import SHEET_CONFIGS
from src.cache import load_cache, save_cache

ROOT_DIR = Path(__file__).parent.parent
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


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

        print("\nWelcome to the rzv_de telegram stats bot!")
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
    cache_path = os.path.join(ROOT_DIR, config.cache_file)
    PROCESSED_AT = datetime.now(config.timezone)
    start_date, end_date = config.get_date_range()

    await print_welcome_msg(config)
    cached_data = load_cache(cache_path)

    if cached_data:
        logger.info("Loading from cache")
        logger.debug(
            f"Cached data channels count: {len(cached_data.get('channels', []))}"
        )
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

            progress_bar = tqdm(
                config.channels["channels"],
                desc="Channel",
                bar_format="Processing channel {desc}: {bar} | {percentage:3.0f}% | {n_fmt}/{total_fmt}",
                ncols=100,
            )

            for channel_id in progress_bar:
                try:
                    channel = await client.get_entity(channel_id)
                    # Update tqdm description with current channel name
                    progress_bar.set_description_str(channel.title)

                    await asyncio.sleep(5)
                    stats = await get_channel_stats(client, channel_id, config.timezone)
                    if stats:
                        all_stats["channels"].append(stats)
                except Exception as e:
                    logger.error(
                        f"Error processing channel {mask_channel_link(channel_id)}: {e}"
                    )
                    continue

            for chat_id in tqdm(config.channels["chats"], desc="Processing chats"):
                try:
                    await asyncio.sleep(2)
                    stats = await get_chat_stats(
                        client, chat_id, config.timezone, start_date, end_date
                    )
                    if stats:
                        all_stats["chats"].append(stats)
                except Exception as e:
                    logger.error(
                        f"Error processing chat {stats['topic_data']['title']}: {e}"
                    )
                    continue

            save_cache(all_stats, cache_path)

    storage = SheetStorage(config.credentials_path, config.sheet_url)

    channels_daily = [
        {
            "channel_id": c["channel_id"],
            "channel_name": c["channel_name"],
            "member_count": c["member_count"],
            "messages_count": len(c["messages"]),
            "processed_at": PROCESSED_AT,
        }
        for c in all_stats["channels"]
    ]
    logger.debug(f"Prepared channels_daily data: {len(channels_daily)} records")

    storage.merge_data(
        "channels_daily", channels_daily, SHEET_CONFIGS["channels_daily"]
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
    logger.debug(f"Prepared channel_messages data: {len(messages)} records")
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

    # if os.path.exists(cache_path):
    # os.remove(cache_path)
    # logger.info("Cache cleared")


if __name__ == "__main__":
    asyncio.run(main())
