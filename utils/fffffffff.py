import spacy
from spacy.matcher import PhraseMatcher
from sentence_transformers import SentenceTransformer, util

def chunk_text(text: str, chunk_size: int = 500):
    """
    Splits the input text into chunks of approximately chunk_size characters.

    This helps in processing large texts by splitting into manageable parts.
    """
    return [text[i:i + chunk_size] for i in range(0, len(text), chunk_size)]

def semantic_phrase_matching_large_text(text: str, phrases: list[str], similarity_threshold: float = 0.7, chunk_size: int = 500) -> int:
    """
    Handles large text by splitting into chunks and performing semantic phrase matching on each chunk.
    
    Args:
    text (str): The large input text to search for phrases.
    phrases (list[str]): A list of key phrases to match.
    similarity_threshold (float): The threshold for semantic similarity matching.
    chunk_size (int): Size of each text chunk to process.

    Returns:
    int: The total count of matched phrases (both exact and semantic).
    """
    nlp = spacy.load('en_core_web_sm')
    model = SentenceTransformer('paraphrase-MiniLM-L6-v2')
    
    # Break the large text into manageable chunks
    text_chunks = chunk_text(text, chunk_size)
    total_matches = 0
    
    for chunk in text_chunks:
        doc = nlp(chunk)

        # Initialize PhraseMatcher for exact phrase matching
        matcher = PhraseMatcher(nlp.vocab, attr="LOWER")
        
        # Add the key phrases to the matcher
        positive_patterns = [nlp(phrase) for phrase in phrases]
        matcher.add("KEY_PHRASES", None, *positive_patterns)
        
        # Check for positive matches in the chunk
        matches = matcher(doc)
        total_matches += len(matches)  # Add exact matches count

        # Fallback to semantic similarity if no exact matches
        text_embedding = model.encode(chunk, convert_to_tensor=True)
        
        for phrase in phrases:
            phrase_embedding = model.encode(phrase, convert_to_tensor=True)
            similarity = util.pytorch_cos_sim(text_embedding, phrase_embedding).item()
            
            if similarity > similarity_threshold:
                total_matches += 1  # Count semantic matches
    
    return total_matches

# Example usage with large text:
large_text = "Your long document content here..."
phrases = ["artificial intelligence", "machine learning", "AI"]
result = semantic_phrase_matching_large_text(large_text, phrases)
print(f"Total matches: {result}")  # Outputs the total number of matches
