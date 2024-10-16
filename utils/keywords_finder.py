import logging
import re
import spacy
import numpy as np
import yake
from rake_nltk import Rake
import unicodedata
import ahocorasick
import utils.keywords as kw
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from functools import lru_cache

# Initialize NLP tools and models
nlp = spacy.load("en_core_web_md")  # 'en_core_web_lg' can also be used
# Initialize YAKE and RAKE
kw_extractor = yake.KeywordExtractor()
r = Rake()
vectorizer = TfidfVectorizer()

# Cache to store phrase embeddings
phrase_embedding_cache = {}

# Initialize the Aho-Corasick automaton
def build_automaton(keywords):
    automaton = ahocorasick.Automaton()
    for idx, keyword in enumerate(keywords):
        automaton.add_word(keyword.lower(), (idx, keyword))
    automaton.make_automaton()
    return automaton

keyword_automaton = build_automaton(kw.KEYWORDS)
irrelevant_automaton = build_automaton(kw.irrelevant_for_keywords)
anti_keyword_automaton = build_automaton(kw.ANTI_KEYWORDS)

def extract_keywords(text):
    # Extract keywords using YAKE
    keywords = [kw for kw, score in kw_extractor.extract_keywords(text)]
    # Extract RAKE phrases
    r.extract_keywords_from_text(text)
    ranked_phrases = r.get_ranked_phrases()
    return list(set(keywords + ranked_phrases))  # Combine and deduplicate

def tfidf_similarity(phrases, text):
    # Vectorize the phrases and the text using TF-IDF
    all_text = phrases + [text]
    tfidf_matrix = vectorizer.fit_transform(all_text)
    # Calculate cosine similarity between the text and each phrase
    similarities = cosine_similarity(tfidf_matrix[-1], tfidf_matrix[:-1])[0]
    return similarities

def tfidf_similarity(phrases, text):
    all_text = phrases + [text]
    tfidf_matrix = vectorizer.fit_transform(all_text)
    similarities = cosine_similarity(tfidf_matrix[-1], tfidf_matrix[:-1])[0]
    return similarities

def smart_kay_prace_maching(text, phrases, irrelevant_phrases, similarity_threshold=0.5):
    keyword_output = []
    total_score = 0
    text = text.lower()

    # Extract keywords using YAKE and RAKE
    keywords = extract_keywords(text)
    combined_keywords = list(set(keywords))

    for keyword in combined_keywords:
        phrase_similarities = tfidf_similarity(phrases, keyword)
        irrelevant_similarities = tfidf_similarity(irrelevant_phrases, keyword)

        max_similarity = max(phrase_similarities)
        max_irrelevant_similarity = max(irrelevant_similarities)

        if max_similarity >= similarity_threshold and max_similarity > max_irrelevant_similarity:
            best_match_index = np.argmax(phrase_similarities)
            keyword_output.append(phrases[best_match_index])
            total_score += max_similarity

    return total_score, keyword_output

def normalize_text(text):
    if not text:
        return ""
    text = re.sub(r"[-_\\n]", " ", text)
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("utf-8")
    text = re.sub(r"[^\w\s]", "", text)
    return re.sub(r"\s+", " ", text).strip().lower()

def find_keyword_positions(text, automaton, irrelevant_automaton, relevant_window):
    found_keywords = []
    text = text.lower()
    for end_index, (idx, keyword) in automaton.iter(text):
        start_index = end_index - len(keyword) + 1
        surrounding_text = text[max(0, start_index - relevant_window): end_index + relevant_window]

        if not contains_irrelevant(irrelevant_automaton, surrounding_text):
            found_keywords.append((keyword, start_index, end_index))
    return found_keywords

def contains_irrelevant(automaton, segment):
    for end_index, (idx, phrase) in automaton.iter(segment):
        return True
    return False

def relative_keywords_score(text: str, bypass_anit=False):
    text = normalize_text(text)
    if not text:
        return 0, [], []
    
    relevant_window = 50
    found_keywords = find_keyword_positions(text, keyword_automaton, irrelevant_automaton, relevant_window)
    processed_positions = set()
    score = calculate_keyword_score(found_keywords, processed_positions, len(text))

    if (not bypass_anit) and score > 0:
        found_anti_keywords = find_keyword_positions(text, anti_keyword_automaton, irrelevant_automaton, relevant_window)
        anti_score = calculate_keyword_score(found_anti_keywords, processed_positions, len(text))
    else:
        found_anti_keywords = []
        anti_score = 0
    
    final_score = score 
    # if final_score <= 0:
    #     try:
    #         score, found_keywords = smart_kay_prace_maching(text, kw.KEYWORDS, kw.irrelevant_for_keywords)
    #         final_score = score - anti_score
    #     except Exception as e:
    #         print(f"Error during processing: {e}")

    return final_score, list(set(found_keywords)), list(set(found_anti_keywords))

def calculate_keyword_score(found_keywords, processed_positions, text_length):
    score = 0
    keyword_frequencies = {keyword: 0 for keyword, _, _ in found_keywords}

    for keyword, start, end in found_keywords:
        keyword_frequencies[keyword] += 1

        if not any(pos in processed_positions for pos in range(start, end)):
            keyword_weight = len(keyword.split())
            frequency_boost = keyword_frequencies[keyword]
            if frequency_boost == 0:
                frequency_boost = 1
            positional_boost = max(1, (text_length - start) / text_length * 2)

            logging.info(f"Keyword: {keyword}, Frequency Boost: {frequency_boost}, Positional Boost: {positional_boost}")

            score += keyword_weight * frequency_boost * positional_boost
            processed_positions.update(range(start, end))

    return score
