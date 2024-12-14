from telethon import functions, errors
import logging
from datetime import datetime, timedelta
import asyncio
from tqdm import tqdm
from src.telegram.utils import mask_channel_link, clean_text, DateFilter
from src.telegram.rate_limiter import TelegramManager

logger = logging.getLogger(__name__)


async def get_messages_by_hour(
    client, chat, topic_id, topic_name, timezone, start_date=None, end_date=None
):
    manager = TelegramManager(client)
    messages_by_hour = {}
    total_messages = 0

    if not start_date:
        end_date = datetime.now(timezone)
        start_date = end_date - timedelta(days=1)

    logger.info(f"Starting messages collection for topic '{topic_name}' between dates '{start_date}' and '{end_date}'")
    message_filter = DateFilter(start_date, end_date)

    try:
        async for message in client.iter_messages(
            chat, reply_to=topic_id, reverse=True, filter=message_filter
        ):
            total_messages += 1
            if total_messages % 1000 == 0:
                logger.info(f"Processed {total_messages} messages for the topic")

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

            await asyncio.sleep(0.1)

    except Exception as e:
        logger.error(f"Error collecting messages for topic '{topic_name}': {str(e)}")

    logger.info(f"Completed with {total_messages} messages")
    return messages_by_hour


async def get_chat_stats(client, chat_id, timezone, start_date=None, end_date=None):
    manager = TelegramManager(client)

    try:
        masked_id = mask_channel_link(chat_id)
        chat = await manager.execute_with_retry(
            client.get_entity, chat_id, entity=chat_id
        )

        stats = {
            "chat_id": masked_id,
            "chat_name": chat.title,
            "timestamp": datetime.now(timezone),
            "topics": {},
        }

        async def get_forum_topics():
            return await client(
                functions.channels.GetForumTopicsRequest(
                    channel=chat, offset_date=0, offset_id=0, offset_topic=0, limit=100
                )
            )

        result = await manager.execute_with_retry(get_forum_topics, chat_id)

        for topic in tqdm(result.topics, desc=f"Processing topics in {chat.title}"):
            try:
                messages = await get_messages_by_hour(
                    client, chat, topic.id, topic.title, timezone, start_date, end_date
                )
                stats["topics"][topic.id] = {"title": topic.title, "messages": messages}
                await asyncio.sleep(1)
            except Exception as e:
                logger.error(
                    f"Error processing topic {topic.title} in {chat.title}: {str(e)}"
                )
                continue

        return stats

    except Exception as e:
        logger.error(f"Failed to get stats for chat {masked_id}: {str(e)}")
        return None


async def get_channel_stats(client, channel_id, timezone):
    manager = TelegramManager(client)

    try:
        channel = await manager.execute_with_retry(
            client.get_entity, channel_id, entity=channel_id
        )

        stats = {
            "channel_id": mask_channel_link(channel_id),
            "channel_name": channel.title,
            "timestamp": datetime.now(timezone),
            "messages": [],
            "member_count": 0,
        }

        participants = await manager.execute_with_retry(
            client.get_participants, channel_id, entity=channel
        )
        stats["member_count"] = len(participants) if participants else 0

        messages = []
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
            await asyncio.sleep(0.1)

        stats["messages"] = messages
        return stats

    except Exception as e:
        logger.error(
            f"Failed to get stats for channel {mask_channel_link(channel_id)}: {str(e)}"
        )
        return None


async def get_channel_names(client, channel_list):
    manager = TelegramManager(client)
    names = {}
    for channel_id in channel_list:
        try:
            entity = await manager.execute_with_retry(
                client.get_entity, channel_id, entity=channel_id
            )
            names[channel_id] = entity.title
        except Exception as e:
            logger.error(f"Error getting name for {mask_channel_link(channel_id)}: {e}")
            names[channel_id] = mask_channel_link(channel_id)
    return names
