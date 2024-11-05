import logging
import re
import spacy
import unicodedata
import ahocorasick
from concurrent.futures import ThreadPoolExecutor, as_completed
import nltk
from nltk.stem import SnowballStemmer
import utils.keywords as kw

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Download necessary NLTK data files (only required once)
nltk.download('punkt')

class KeypaceFinder:
    def __init__(self, keywords=kw.KEYWORDS):
        # Initialize spaCy model and NLTK stemmer
        try:
            self.nlp = spacy.load("en_core_web_sm")
        except OSError:
            # Download model if not present
            from spacy.cli import download
            download("en_core_web_sm")
            self.nlp = spacy.load("en_core_web_sm")

        self.stemmer = SnowballStemmer("english")
        self.keypace_automaton = self.build_automaton(keywords)

    @staticmethod
    def build_automaton(keyphrases):
        """Builds an Aho-Corasick automaton for efficient keyword searching."""
        automaton = ahocorasick.Automaton()
        for idx, phrase in enumerate(keyphrases):
            automaton.add_word(phrase.lower(), (idx, phrase))
        automaton.make_automaton()  # Finalize the automaton
        logger.info("Aho-Corasick automaton built with %d keywords.", len(keyphrases))
        return automaton

    def normalize_text(self, text):
        """Normalizes text by removing special characters, accents, and extra spaces."""
        if not text:
            return ""
        # Replace specific characters with spaces, remove accents, and clean punctuation
        text = re.sub(r"[-_\n]", " ", text)
        text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("utf-8")
        text = re.sub(r"[^\w\s]", "", text)
        text = re.sub(r"\s+", " ", text).strip().lower()
        return self.stem_phrase(text)

    def stem_phrase(self, phrase):
        """Stems each word in a phrase using NLTK's SnowballStemmer."""
        words = phrase.split()
        stemmed_phrase = " ".join([self.stemmer.stem(word) for word in words])
        return stemmed_phrase

    def find_keypaces(self, text):
        """Finds key phrases in the text using the Aho-Corasick automaton."""
        found_keypaces = []
        text_lower = text.lower()
        for end_index, (idx, phrase) in self.keypace_automaton.iter(text_lower):
            start_index = end_index - len(phrase) + 1
            found_keypaces.append((phrase, start_index, end_index))
        return found_keypaces

    def relative_keywords_score(self, text):
        """Calculates the number of unique key phrases in the text and returns the score."""
        normalized_text = self.normalize_text(text)
        if not normalized_text:
            return 0, [], []

        found_keypaces = self.find_keypaces(normalized_text)
        
        # Extract only unique key phrases for scoring
        found_keypaces_only = [phrase for phrase, _, _ in found_keypaces]
        unique_keypaces = list(set(found_keypaces_only))  # Deduplicate

        return len(unique_keypaces), unique_keypaces, found_keypaces


# Example usage:
if __name__ == "__main__":
    sample_text = "Your sample text goes here."
    finder = KeypaceFinder()
    score, keypaces, _ = finder.relative_keywords_score(sample_text)
    print(f"Score: {score}, Keypaces: {keypaces}")
