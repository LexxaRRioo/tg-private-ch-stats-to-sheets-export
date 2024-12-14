import pymorphy3
from nltk.stem import WordNetLemmatizer
import logging
from nltk import pos_tag
from nltk.corpus import wordnet
import nltk

logger = logging.getLogger(__name__)


# Temporarily disable nltk logging during downloads
def download_nltk_data():
    nltk_logger = logging.getLogger("nltk")
    original_level = nltk_logger.level

    try:
        nltk_logger.setLevel(logging.ERROR)
        nltk.download("averaged_perceptron_tagger", quiet=True)
        nltk.download("wordnet", quiet=True)
        nltk.download("universal_tagset", quiet=True)
    finally:
        nltk_logger.setLevel(original_level)


def get_wordnet_pos(word):
    """Map POS tag to first character lemmatize() accepts"""
    tag = pos_tag([word])[0][1][0].upper()
    tag_dict = {
        "J": wordnet.ADJ,
        "N": wordnet.NOUN,
        "V": wordnet.VERB,
        "R": wordnet.ADV,
    }
    return tag_dict.get(tag, wordnet.NOUN)  # Default to NOUN if tag not found


class WordNormalizer:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            logger.info("Initializing WordNormalizer (singleton)")
            download_nltk_data()
            cls._instance.ru_morph = pymorphy3.MorphAnalyzer()
            cls._instance.en_lemmatizer = WordNetLemmatizer()
        return cls._instance

    def normalize_word(self, word, is_russian):
        try:
            if is_russian:
                # For Russian, parse and get normal form
                # Get all possible parsing variants
                parses = self.ru_morph.parse(word)
                if parses:
                    # Try to get a noun form if exists
                    noun_parses = [p for p in parses if "NOUN" in p.tag]
                    if noun_parses:
                        return noun_parses[0].normal_form
                    # Otherwise get the normal form of first parse
                    return parses[0].normal_form
            else:
                # For English, use POS tagging for better lemmatization
                pos = get_wordnet_pos(word)
                return self.en_lemmatizer.lemmatize(word, pos)

            return word
        except Exception as e:
            logger.error(f"Error normalizing word '{word}': {e}")
            return word

    def debug_normalize(self, word, is_russian):
        """Helper method for debugging normalization process"""
        if is_russian:
            parses = self.ru_morph.parse(word)
            print(f"\nDebug for word '{word}':")
            for p in parses:
                print(f"  Parse: {p.normal_form} ({p.tag})")
        else:
            pos = get_wordnet_pos(word)
            norm = self.en_lemmatizer.lemmatize(word, pos)
            print(f"\nDebug for word '{word}':")
            print(f"  POS: {pos}")
            print(f"  Normalized: {norm}")
