from telethon import functions, errors
import logging
from datetime import datetime, timedelta
import asyncio
from tqdm import tqdm
from src.telegram.utils import mask_channel_link, clean_text
from src.telegram.rate_limiter import TelegramManager

logger = logging.getLogger(__name__)


async def get_messages_by_hour(
    client,
    chat,
    topic_id,
    topic_name,
    timezone,
    storage=None,
    mode="regular",
    original_chat_id=None,
):
    messages_by_hour = {}
    total_messages = 0
    chunk_size = 1000

    try:
        latest = await client.get_messages(chat, limit=1, reply_to=topic_id)
        if not latest:
            logger.info(f"No messages found for topic '{topic_name}'")
            return messages_by_hour

        latest_id = latest[0].id

        # Determine start_id based on mode
        if mode == "regular" and storage:
            # Use original invite link that was passed to the script
            chat_id = mask_channel_link(original_chat_id)
            last_id = storage.get_last_message_id(chat_id, topic_id)
            start_id = max(0, (last_id or 0) - 100)  # Safety margin of 100 messages
        else:
            start_id = 0

        logger.info(
            f"Topic '{topic_name}': collecting messages from ID {start_id} to {latest_id}"
        )

        # Handle large gaps with chunks
        if latest_id - start_id > 5000:
            logger.info(
                f"Large message gap detected ({latest_id - start_id} messages), using chunks"
            )
            chunks = range(start_id, latest_id, chunk_size)
        else:
            chunks = [start_id]

        for chunk_start in chunks:
            async for message in client.iter_messages(
                chat, reply_to=topic_id, min_id=chunk_start, reverse=True
            ):
                total_messages += 1
                if total_messages % 1000 == 0:
                    logger.info(
                        f"Processed {total_messages} messages for topic '{topic_name}'"
                    )

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

    logger.info(f"Topic '{topic_name}': completed with {total_messages} message(s)")
    return messages_by_hour


async def get_chat_stats(client, chat_id, timezone, storage=None, mode="regular"):
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

        result = await manager.execute_with_retry(
            lambda: client(
                functions.channels.GetForumTopicsRequest(
                    channel=chat, offset_date=0, offset_id=0, offset_topic=0, limit=100
                )
            ),
            chat_id,
        )

        # Setup progress bars with proper formatting
        topic_progress = tqdm(
            total=len(result.topics),
            desc=f"Processing topics in {chat.title}",
            position=1,  # Place below chat progress
            leave=False,  # Don't leave in output
            ncols=80,  # Fixed width
            bar_format="{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]",
        )

        for topic in result.topics:
            try:
                messages = await get_messages_by_hour(
                    client,
                    chat,
                    topic.id,
                    topic.title,
                    timezone,
                    storage=storage,
                    mode=mode,
                    original_chat_id=chat_id,
                )
                stats["topics"][topic.id] = {"title": topic.title, "messages": messages}
                topic_progress.update(1)  # Update progress
                await asyncio.sleep(1)
            except Exception as e:
                logger.error(
                    f"Error processing topic {topic.title} in {chat.title}: {str(e)}"
                )
                continue

        topic_progress.close()  # Properly close progress bar
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
