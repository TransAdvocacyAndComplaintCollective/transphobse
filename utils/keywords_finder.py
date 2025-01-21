import logging
import re
import unicodedata
import ahocorasick

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class KeypaceFinder:
    def __init__(self, keywords: list[str]):
        self.keywords = keywords
        self.keypace_automaton = self.build_automaton(keywords)

    @staticmethod
    def build_automaton(keyphrases):
        """Builds an Aho-Corasick automaton for efficient keyword searching."""
        automaton = ahocorasick.Automaton()
        for idx, phrase in enumerate(keyphrases):
            automaton.add_word(phrase.lower(), (idx, phrase))
        automaton.make_automaton()
        logger.info("Aho-Corasick automaton built with %d keywords.", len(keyphrases))
        return automaton

    def normalize_text(self, text):
        """Normalizes text by removing special characters, accents, and extra spaces."""
        if not text:
            return "", text

        if isinstance(text, bytes):
            try:
                text = text.decode('utf-8')
            except UnicodeDecodeError as e:
                logger.error("Failed to decode bytes: %s", e)
                return "", text

        original_text = text
        text = unicodedata.normalize("NFKD", text)
        text = ''.join(c for c in text if not unicodedata.combining(c))
        text = re.sub(r"[^\w\s]", "", text)
        text = re.sub(r"\s+", " ", text).strip().lower()

        return text, original_text

    def find_keypaces(self, text):
        """Finds key phrases in the text using the Aho-Corasick automaton with word boundaries."""
        found_keypaces = []
        text_lower = text.lower()
        for end_index, (idx, phrase) in self.keypace_automaton.iter(text_lower):
            start_index = end_index - len(phrase) + 1
            if ((start_index == 0 or not text_lower[start_index - 1].isalnum()) and
                (end_index == len(text_lower) - 1 or not text_lower[end_index + 1].isalnum())):
                found_keypaces.append((phrase, start_index, end_index))
        return found_keypaces

    def relative_keywords_score(self, text):
        """
        Calculates the number of unique key phrases and total occurrences in the text.
        Returns:
            unique_count (int): Number of unique key phrases found.
            unique_keypaces (List[str]): List of unique key phrases.
            found_keypaces (List[Tuple[str, int, int]]): List of all occurrences with positions.
            total_count (int): Total number of keyword occurrences found.
        """
        normalized_text, original_text = self.normalize_text(text)
        logger.debug(f"Normalized Text: {normalized_text}")
        logger.debug(f"Original Text: {original_text}")

        # Use normalized text for matching
        found_keypaces = self.find_keypaces(normalized_text)
        found_keypaces_only = [phrase for phrase, _, _ in found_keypaces]
        unique_keypaces = list(set(found_keypaces_only))
        unique_count = len(unique_keypaces)
        total_count = len(found_keypaces)

        return unique_count, unique_keypaces, found_keypaces, total_count

# Example usage:
if __name__ == "__main__":
    keywords = ["trans rights", "gender equality", "non-binary", "LGBTQ+", "transgender"]
    sample_text = "Your sample text goes here, mentioning Trans Rights and Gender Equality."

    finder = KeypaceFinder(keywords)
    unique_count, unique_keypaces, occurrences, total_count = finder.relative_keywords_score(sample_text)
    print(f"Unique Count: {unique_count}")
    print(f"Unique Keypaces: {unique_keypaces}")
    print(f"Total Occurrences: {total_count}")

