from telethon import functions, errors
import logging
from datetime import datetime
import asyncio
from tqdm import tqdm
from src.telegram.utils import mask_channel_link, clean_text
from collections import Counter

logger = logging.getLogger(__name__)


async def get_messages_by_hour(client, chat, topic_id, topic_title, timezone):
    messages_by_hour = {}
    total_messages = 0

    logger.info(f"Starting messages collection for topic '{topic_title}'")
    async for message in client.iter_messages(chat, reply_to=topic_id, reverse=True):
        total_messages += 1
        if total_messages % 1000 == 0:
            logger.info(f"Processed {total_messages} messages for the topic '{topic_title}'")

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

    logger.info(f"Completed topic '{topic_title}' with {total_messages} messages")
    return messages_by_hour


async def get_chat_stats(client, chat_id, timezone):
    max_retries = 3
    for retry in range(max_retries):
        try:
            masked_id = mask_channel_link(chat_id)
            chat = await client.get_entity(chat_id)
            stats = {
                "chat_id": masked_id,
                "chat_name": chat.title,
                "timestamp": datetime.now(timezone),
                "topics": {},
            }

            result = await client(
                functions.channels.GetForumTopicsRequest(
                    channel=chat, offset_date=0, offset_id=0, offset_topic=0, limit=100
                )
            )

            # Main progress bar for topics
            with tqdm(
                total=len(result.topics),
                desc=f"Processing chat '{chat.title}'",
                position=1,  # Main progress at top
                leave=False,  # Keep the bar after completion
                ncols=80
            ) as pbar:
                for topic in result.topics:
                    messages = await get_messages_by_hour(
                        client, chat, topic.id, topic.title, timezone
                    )
                    stats["topics"][topic.id] = {"title": topic.title, "messages": messages}
                    await asyncio.sleep(1)
                    pbar.update(1)

            return stats

        except errors.FloodWaitError as e:
            if retry == max_retries - 1:
                raise
            await asyncio.sleep(e.seconds)
        except Exception as e:
            logger.error(f"Error getting chat stats for {masked_id}: {e}")
            return None


async def get_channel_stats(client, channel_id, timezone):
    max_retries = 3
    for retry in range(max_retries):
        try:
            masked_id = mask_channel_link(channel_id)
            channel = await client.get_entity(channel_id)
            stats = {
                "channel_id": masked_id,
                "channel_name": channel.title,
                "timestamp": datetime.now(timezone),
                "messages": [],
                "member_count": 0,
            }

            participants = await client.get_participants(channel, limit=0)
            stats["member_count"] = participants.total

            messages = []
            hashtag_occurrences = []

            async for message in client.iter_messages(channel, limit=100):
                if message.text:
                    # Extract hashtags from the text
                    message_hashtags = [word for word in message.text.split() if word.startswith('#')]
                    msg_date = message.date.astimezone(timezone)
                    
                    message_data = {
                        "date": msg_date.strftime("%Y-%m-%dT%H:%M:%S"),
                        "text": message.text,
                        "processed_text": clean_text(message.text),
                        "message_id": message.id,
                        "hashtags": message_hashtags
                    }
                    messages.append(message_data)
                    
                    # Store each hashtag occurrence separately
                    for hashtag in message_hashtags:
                        hashtag_occurrences.append({
                            "message_id": message.id,
                            "date": msg_date.strftime("%Y-%m-%dT%H:%M:%S"),
                            "hashtag": hashtag
                        })

            stats["messages"] = messages
            stats["hashtag_occurrences"] = hashtag_occurrences

            return stats

        except errors.FloodWaitError as e:
            if retry == max_retries - 1:
                raise
            await asyncio.sleep(e.seconds)
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
                break
            except errors.FloodWaitError as e:
                if retry == max_retries - 1:
                    raise
                await asyncio.sleep(e.seconds)
            except Exception as e:
                logger.error(
                    f"Error getting name for {mask_channel_link(channel_id)}: {e}"
                )
                names[channel_id] = mask_channel_link(channel_id)
    return names
