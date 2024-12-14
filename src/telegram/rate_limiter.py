import asyncio
import logging
from datetime import datetime, timedelta
from telethon.errors import (
    FloodWaitError,
    ChatAdminRequiredError,
    UserNotParticipantError,
    ChannelPrivateError,
    SlowModeWaitError,
    TimeoutError,
)

logger = logging.getLogger(__name__)


class TelegramRateLimiter:
    def __init__(self):
        self.last_request_time = {}
        self.default_delay = 5
        self.backoff_factor = 1.5
        self.max_retries = 3
        self.flood_wait_retries = {}

    async def wait_before_request(self, channel_id):
        """Implement rate limiting per channel"""
        if channel_id in self.last_request_time:
            elapsed = datetime.now() - self.last_request_time[channel_id]
            if elapsed.total_seconds() < self.default_delay:
                await asyncio.sleep(self.default_delay - elapsed.total_seconds())
        self.last_request_time[channel_id] = datetime.now()

    async def handle_flood_wait(self, e: FloodWaitError, channel_id: str, retry: int):
        """Handle Telegram's flood wait errors with per-channel tracking"""
        wait_time = e.seconds
        if channel_id not in self.flood_wait_retries:
            self.flood_wait_retries[channel_id] = 0

        self.flood_wait_retries[channel_id] += 1

        if self.flood_wait_retries[channel_id] <= self.max_retries:
            logger.warning(
                f"FloodWaitError: Waiting for {wait_time} seconds before retry {self.flood_wait_retries[channel_id]}/{self.max_retries}"
            )
            await asyncio.sleep(wait_time)
            return True

        logger.error(
            f"Max retries exceeded for FloodWaitError on channel {channel_id} (required wait: {wait_time}s)"
        )
        return False


class TelegramManager:
    def __init__(self, client, rate_limiter=None):
        self.client = client
        self.rate_limiter = rate_limiter or TelegramRateLimiter()
        self.timeout = 30

    async def execute_with_retry(self, operation, channel_id, **kwargs):
        """Execute Telegram API operation with retry logic"""
        retry = 0
        while retry <= self.rate_limiter.max_retries:
            try:
                await self.rate_limiter.wait_before_request(channel_id)
                return await asyncio.wait_for(operation(**kwargs), timeout=self.timeout)

            except FloodWaitError as e:
                should_retry = await self.rate_limiter.handle_flood_wait(
                    e, channel_id, retry
                )
                if not should_retry:
                    break
                retry += 1

            except TimeoutError:
                if retry < self.rate_limiter.max_retries:
                    wait_time = self.rate_limiter.default_delay * (
                        self.rate_limiter.backoff_factor**retry
                    )
                    logger.warning(
                        f"Timeout, retrying in {wait_time:.1f} seconds ({retry + 1}/{self.rate_limiter.max_retries})"
                    )
                    await asyncio.sleep(wait_time)
                    retry += 1
                else:
                    logger.error("Max retries exceeded for timeout")
                    raise

            except (
                ChatAdminRequiredError,
                UserNotParticipantError,
                ChannelPrivateError,
            ) as e:
                logger.error(f"Access error for channel {channel_id}: {str(e)}")
                raise

            except Exception as e:
                logger.error(f"Unexpected error: {str(e)}")
                if retry < self.rate_limiter.max_retries:
                    wait_time = self.rate_limiter.default_delay * (
                        self.rate_limiter.backoff_factor**retry
                    )
                    logger.warning(
                        f"Retrying in {wait_time:.1f} seconds ({retry + 1}/{self.rate_limiter.max_retries})"
                    )
                    await asyncio.sleep(wait_time)
                    retry += 1
                else:
                    raise

        raise Exception(
            f"Failed to execute operation after {retry} retries for channel {channel_id}"
        )
