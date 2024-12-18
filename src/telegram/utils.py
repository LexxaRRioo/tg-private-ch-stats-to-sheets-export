import re
from telethon.tl.types import InputMessagesFilterEmpty


class DateFilter(InputMessagesFilterEmpty):
    def __init__(self, start_date, end_date):
        self.start_date = start_date
        self.end_date = end_date

    def filter(self, message):
        return self.start_date <= message.date <= self.end_date


def clean_text(text):
    """Clean text for word cloud"""
    text = re.sub(r"http\S+|www\S+|https\S+", "", text, flags=re.MULTILINE)
    text = re.sub(r"[-*?()\"'\+;\.\,:`<>\#\[\]%\(\)]+|[?!]+$", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip().lower()


def mask_channel_link(link):
    """Mask parts of channel link for privacy"""
    if not link:
        return link
    if "+" in link:
        base, hash_part = link.split("+")
        return f"{base}+{'*' * (len(hash_part) // 2)}{hash_part[len(hash_part) // 2:]}"
    parts = link.split("/")
    if len(parts) > 1:
        channel_name = parts[-1]
        return f"{parts[0]}//{parts[2]}/{'*' * (len(channel_name) // 2)}{channel_name[len(channel_name) // 2:]}"
    return link
