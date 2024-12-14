import re
from telethon.tl.types import InputMessagesFilterEmpty
from src.nlp.normalizer import WordNormalizer
import logging
import pytz

logger = logging.getLogger(__name__)

word_normalizer = WordNormalizer()


def clean_text(text):
    """Clean and normalize text for word cloud"""
    if not text:
        return ""

    try:
        # Basic cleaning
        text = re.sub(r"http\S+|www\S+|https\S+", "", text, flags=re.MULTILINE)
        text = re.sub(r"[-*?()\"'\+;\.\,:`<>\#\[\]%\(\)]+|[?!]+$", " ", text)
        text = re.sub(r"\s+", " ", text)
        text = text.strip().lower()

        # Normalize words
        normalized = []
        for word in text.split():
            if not word:  # Skip empty strings
                continue
            # Check if word contains Russian characters
            is_russian = any(c in "абвгдеёжзийклмнопрстуфхцчшщъыьэюя" for c in word)
            norm_word = word_normalizer.normalize_word(word, is_russian)
            if norm_word:  # Only add non-empty normalized words
                normalized.append(norm_word)

        return " ".join(normalized)
    except Exception as e:
        logger.error(f"Error in clean_text: {e}")
        return text  # Return original text if something goes wrong


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
