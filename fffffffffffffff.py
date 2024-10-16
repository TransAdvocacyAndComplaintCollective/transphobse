

# Core matching function modified to use Ball Tree
import numpy as np
from sklearn.neighbors import BallTree

import logging
import re
import spacy
import numpy as np
from sentence_transformers import SentenceTransformer, util
import yake
from rake_nltk import Rake
import unicodedata
import ahocorasick
from sklearn.neighbors import BallTree
from functools import lru_cache
# Initialize NLP tools and models
nlp = spacy.load("en_core_web_md")  # 'en_core_web_lg' can also be used
model = SentenceTransformer("paraphrase-MiniLM-L6-v2")
# Initialize YAKE and RAKE
kw_extractor = yake.KeywordExtractor()
r = Rake()

# Cache to store phrase embeddings
phrase_embedding_cache = {}


# Function to cache phrase embeddings
def get_phrase_embedding(phrase: str):
    if phrase not in phrase_embedding_cache:
        phrase_embedding_cache[phrase] = model.encode(phrase, convert_to_tensor=True, show_progress_bar=False)
    return phrase_embedding_cache[phrase]

def extract_keywords(text):
    # Extract keywords using YAKE
    keywords = [kw for kw, score in kw_extractor.extract_keywords(text)]
    # Extract RAKE phrases
    r.extract_keywords_from_text(text)
    ranked_phrases = r.get_ranked_phrases()
    return list(set(keywords + ranked_phrases))  # Combine and deduplicate



# Cache the phrase embeddings using the LRU cache decorator
@lru_cache(maxsize=10000)
def cache_phrase_embedding(phrase: str):
    return model.encode(phrase, convert_to_tensor=True, show_progress_bar=False)

def smart_kay_prace_maching_with_ball_tree(
    text: str,
    phrases: list[str],
    irrelevant_phrases: list[str],
    similarity_threshold: float = 0.9,
) -> tuple[int, list[str], list[str]]:
    keyword_output = []
    total_score = 0
    text = text.lower()

    # Extract keywords using YAKE and RAKE
    keywords = extract_keywords(text)
    r.extract_keywords_from_text(text)
    ranked_phrases = r.get_ranked_phrases()

    # Combine keywords and ranked phrases
    combined_keywords = list(set(keywords + ranked_phrases))

    # Batch encode the combined keywords
    text_embeddings = model.encode(combined_keywords, convert_to_tensor=True, show_progress_bar=True)

    # Cache and batch encode the phrases and irrelevant phrases using the LRU cache
    phrase_embeddings = [cache_phrase_embedding(phrase) for phrase in phrases]
    irrelevant_phrase_embeddings = [cache_phrase_embedding(phrase) for phrase in irrelevant_phrases]

    # Convert to NumPy arrays for faster similarity calculations
    phrase_embeddings_np = np.array([emb.cpu().numpy() for emb in phrase_embeddings])
    irrelevant_phrase_embeddings_np = np.array([emb.cpu().numpy() for emb in irrelevant_phrase_embeddings])
    text_embeddings_np = np.array([emb.cpu().numpy() for emb in text_embeddings])

    # Using Ball Tree for efficient similarity lookup
    tree = BallTree(phrase_embeddings_np)

    outputs = {}

    # Calculate similarity between text keywords and phrases using Ball Tree
    for i, text_embedding in enumerate(text_embeddings_np):
        # Query the Ball Tree for neighbors within the similarity threshold
        distances, indices = tree.query_radius([text_embedding], r=1 - similarity_threshold, return_distance=True)

        for dist, idx in zip(distances[0], indices[0]):
            idx = int(idx)  # Convert idx to integer
            if len(combined_keywords[i]) >= 5:
                keyword = combined_keywords[i]
                similarity = 1 - dist
                phrase = phrases[idx]

                outputs[keyword] = {
                    "phrase": phrase,
                    "similarity_good": max(similarity, outputs.get(keyword, {}).get("similarity_good", 0)),
                    "length_phrase": len(phrase),
                    "text_embedding": text_embedding,
                }

    # Check for irrelevant phrases and their similarity using Ball Tree
    irrelevant_tree = BallTree(irrelevant_phrase_embeddings_np)
    for keyword, output in outputs.items():
        distances, _ = irrelevant_tree.query_radius([output["text_embedding"]], r=1 - similarity_threshold, return_distance=True)
        max_bad_similarity = np.max([1 - dist for dist in distances[0]]) if distances[0].size > 0 else 0
        if max_bad_similarity > similarity_threshold:
            outputs[keyword]["similarity_bad"] = max_bad_similarity

    # Calculate final score
    for output in outputs.values():
        score = (output["similarity_good"] - output.get("similarity_bad", 0)) * output["length_phrase"]
        if score > 0:
            total_score += score
            keyword_output.append(output["phrase"])

    return total_score, keyword_output


# Sample text
text = """
Trans rights are human rights. We believe in the importance of supporting trans individuals 
through inclusive policies and practices. However, there are still many challenges and 
discriminatory behaviors that exist in society.
"""

# Sample phrases (these would be your target keywords or key phrases related to the topic)
phrases = [
    "trans rights", 
    "human rights", 
    "supporting trans individuals", 
    "inclusive policies", 
    "discriminatory behaviors", 
    "society"
]

# Irrelevant phrases (these are phrases that, if detected, should reduce the score)
irrelevant_phrases = [
    "discrimination is acceptable", 
    "exclusion policies", 
    "anti-trans", 
    "against trans rights"
]

# Run the smart_kay_prace_maching_with_ball_tree function with the sample data
score, matching_phrases = smart_kay_prace_maching_with_ball_tree(
    text=text,
    phrases=phrases,
    irrelevant_phrases=irrelevant_phrases,
    similarity_threshold=0.9  # Adjust the similarity threshold as needed
)

# Display the results
print(f"Score: {score}")
print("Matching Phrases:", matching_phrases)