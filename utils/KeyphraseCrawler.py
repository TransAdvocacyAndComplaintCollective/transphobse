import sqlite3
import json
import pinecone
from transformers import AutoModel, AutoTokenizer
import torch

class KeyphraseTable:
    def __init__(self, conn, pinecone_api_key=None):
        # Initialize SQLite connection
        self.conn = conn
        self._create_table()

        # Initialize Pinecone
        if pinecone_api_key:
            pinecone.init(api_key=pinecone_api_key, environment="us-west1-gcp")  # Adjust for your region
            self.index = pinecone.Index("keyphrases")  # Name of the Pinecone index
        else:
            self.index = None

        # Initialize Transformer model for vector generation (e.g., BERT)
        self.tokenizer = AutoTokenizer.from_pretrained("sentence-transformers/all-MiniLM-L6-v2")
        self.model = AutoModel.from_pretrained("sentence-transformers/all-MiniLM-L6-v2")

    def _create_table(self):
        """Create the SQL table for storing keyphrases if it doesn't exist."""
        create_table_query = """
        CREATE TABLE IF NOT EXISTS keyphrases (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            keyphrase TEXT UNIQUE,
            tag TEXT,
            type TEXT,
            keywords_matched TEXT,
            anti_keywords_matched TEXT,
            avg_score_correlate REAL,
            avg_score_non_correlate REAL,
            std_dev_score_correlate REAL,
            std_dev_score_non_correlate REAL,
            most_similar_non_correlate_phrase TEXT,
            similarity_score REAL,
            related_keywords TEXT,
            similar_texts TEXT,
            found_keywords_count INTEGER,
            found_anti_keywords_count INTEGER,
            version INTEGER DEFAULT 1
        );
        """
        self.conn.execute(create_table_query)
        self.conn.commit()

    def _generate_vector(self, text):
        """Generate an embedding vector for a given keyphrase using a transformer model."""
        inputs = self.tokenizer(text, return_tensors="pt", padding=True, truncation=True)
        outputs = self.model(**inputs)
        vector = outputs.last_hidden_state.mean(dim=1).squeeze().detach().numpy()
        return vector

    def add_or_update_keyphrase(self, keyphrase_data):
        """Add or update a keyphrase in the SQL table and Pinecone, ensuring uniqueness."""
        # Convert list/dict fields into JSON strings
        keyphrase_data["keywords_matched"] = json.dumps(keyphrase_data["keywords_matched"])
        keyphrase_data["anti_keywords_matched"] = json.dumps(keyphrase_data["anti_keywords_matched"])
        keyphrase_data["related_keywords"] = json.dumps(keyphrase_data["related_keywords"])
        keyphrase_data["similar_texts"] = json.dumps(keyphrase_data["similar_texts"])

        # Check if the keyphrase exists, and if it does, increment its version
        existing_keyphrase = self.get_keyphrase(keyphrase_data["keyphrase"])
        if existing_keyphrase:
            version = existing_keyphrase["version"] + 1
        else:
            version = 1

        # Insert into SQLite database
        insert_query = """
        INSERT INTO keyphrases (keyphrase, tag, type, keywords_matched, anti_keywords_matched, 
            avg_score_correlate, avg_score_non_correlate, std_dev_score_correlate, std_dev_score_non_correlate, 
            most_similar_non_correlate_phrase, similarity_score, related_keywords, similar_texts, 
            found_keywords_count, found_anti_keywords_count, version)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(keyphrase) DO UPDATE SET
            tag=excluded.tag,
            type=excluded.type,
            keywords_matched=excluded.keywords_matched,
            anti_keywords_matched=excluded.anti_keywords_matched,
            avg_score_correlate=excluded.avg_score_correlate,
            avg_score_non_correlate=excluded.avg_score_non_correlate,
            std_dev_score_correlate=excluded.std_dev_score_correlate,
            std_dev_score_non_correlate=excluded.std_dev_score_non_correlate,
            most_similar_non_correlate_phrase=excluded.most_similar_non_correlate_phrase,
            similarity_score=excluded.similarity_score,
            related_keywords=excluded.related_keywords,
            similar_texts=excluded.similar_texts,
            found_keywords_count=excluded.found_keywords_count,
            found_anti_keywords_count=excluded.found_anti_keywords_count,
            version=excluded.version + 1;
        """
        values = (
            keyphrase_data["keyphrase"], keyphrase_data["tag"], keyphrase_data["type"], 
            keyphrase_data["keywords_matched"], keyphrase_data["anti_keywords_matched"], 
            keyphrase_data["avg_score_correlate"], keyphrase_data["avg_score_non_correlate"], 
            keyphrase_data["std_dev_score_correlate"], keyphrase_data["std_dev_score_non_correlate"], 
            keyphrase_data["most_similar_non_correlate_phrase"], keyphrase_data["similarity_score"], 
            keyphrase_data["related_keywords"], keyphrase_data["similar_texts"], 
            keyphrase_data["found_keywords_count"], keyphrase_data["found_anti_keywords_count"], 
            version  # Set the new or incremented version
        )
        self.conn.execute(insert_query, values)
        self.conn.commit()

        # Add or update in Pinecone (Vector DB)
        if self.index:
            vector = self._generate_vector(keyphrase_data["keyphrase"])
            self.index.upsert(vectors=[(keyphrase_data["keyphrase"], vector)])

    def find_similar_keyphrases(self, keyphrase, top_k=5, recursive_top_k=3):
        """Find similar keyphrases using Pinecone and fetch their SQL rows, and for each similar keyphrase, 
        find its own similar keyphrases."""
        
        if not self.index:
            raise ValueError("Pinecone is not initialized.")
        
        # Generate vector for the input keyphrase
        vector = self._generate_vector(keyphrase)
        
        # Query Pinecone for similar keyphrases
        result = self.index.query(vector, top_k=top_k, include_values=True)
        
        # Extract keyphrase IDs from the Pinecone result
        similar_keyphrases = [match["id"] for match in result["matches"]]
        
        # Retrieve the corresponding SQL rows for each similar keyphrase
        similar_keyphrase_data = []
        for keyphrase_id in similar_keyphrases:
            keyphrase_data = self.get_keyphrase(keyphrase_id)
            
            if keyphrase_data:
                # Find similar keyphrases for each similar keyphrase (recursive search)
                keyphrase_vector = self._generate_vector(keyphrase_id)
                recursive_result = self.index.query(keyphrase_vector, top_k=recursive_top_k, include_values=False)
                recursive_similar_keyphrases = [rec_match["id"] for rec_match in recursive_result["matches"]]
                
                # Add similar keyphrases to the result
                keyphrase_data["similar_keyphrases"] = recursive_similar_keyphrases
                similar_keyphrase_data.append(keyphrase_data)
        
        return similar_keyphrase_data

    def get_keyphrase(self, keyphrase):
        """Fetch a keyphrase by its name."""
        select_query = "SELECT * FROM keyphrases WHERE keyphrase = ?"
        cursor = self.conn.execute(select_query, (keyphrase,))
        row = cursor.fetchone()
        if row:
            return self._row_to_dict(row)
        return None
    

    def delete_keyphrase(self, keyphrase):
        """Delete a keyphrase from the SQL table and Pinecone."""
        delete_query = "DELETE FROM keyphrases WHERE keyphrase = ?"
        self.conn.execute(delete_query, (keyphrase,))
        self.conn.commit()

        if self.index:
            self.index.delete(ids=[keyphrase])

    def _row_to_dict(self, row):
        """Convert a row from the SQL result set to a dictionary."""
        return {
            "id": row[0],
            "keyphrase": row[1],
            "tag": row[2],
            "type": row[3],
            "keywords_matched": json.loads(row[4]),
            "anti_keywords_matched": json.loads(row[5]),
            "avg_score_correlate": row[6],
            "avg_score_non_correlate": row[7],
            "std_dev_score_correlate": row[8],
            "std_dev_score_non_correlate": row[9],
            "most_similar_non_correlate_phrase": row[10],
            "similarity_score": row[11],
            "related_keywords": json.loads(row[12]),
            "similar_texts": json.loads(row[13]),
            "found_keywords_count": row[14],
            "found_anti_keywords_count": row[15],
            "version": row[16]
        }

    # Dictionary-like behavior methods
    def __getitem__(self, keyphrase):
        return self.get_keyphrase(keyphrase)

    def __setitem__(self, keyphrase, keyphrase_data):
        keyphrase_data["keyphrase"] = keyphrase
        self.add_or_update_keyphrase(keyphrase_data)

    def __delitem__(self, keyphrase):
        self.delete_keyphrase(keyphrase)

    def __contains__(self, keyphrase):
        return self.get_keyphrase(keyphrase) is not None

    def __len__(self):
        cursor = self.conn.execute("SELECT COUNT(*) FROM keyphrases")
        return cursor.fetchone()[0]

    def keys(self):
        cursor = self.conn.execute("SELECT keyphrase FROM keyphrases")
        return [row[0] for row in cursor.fetchall()]

    def values(self):
        cursor = self.conn.execute("SELECT * FROM keyphrases")
        return [self._row_to_dict(row) for row in cursor.fetchall()]

    def items(self):
        cursor = self.conn.execute("SELECT * FROM keyphrases")
        return [(row[1], self._row_to_dict(row)) for row in cursor.fetchall()]

    def get(self, keyphrase, default=None):
        return self.get_keyphrase(keyphrase) or default

    def pop(self, keyphrase, default=None):
        keyphrase_data = self.get_keyphrase(keyphrase)
        if keyphrase_data:
            self.delete_keyphrase(keyphrase)
            return keyphrase_data
        return default

    def update(self, other):
        for keyphrase, data in other.items():
            self[keyphrase] = data

    def copy(self):
        return dict(self.items())

    def fromkeys(self, keys, value=None):
        return {key: self.get(key, value) for key in keys}

    def clear(self):
        self.conn.execute("DELETE FROM keyphrases")
        self.conn.commit()
