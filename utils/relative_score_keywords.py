import re


def relative_score(self, text):
    """
    Calculate the relevance score of a page or anchor text based on keywords and key phrases,
    handling both relevant and irrelevant keywords with context-aware logic and preventing substring conflicts.
    """
    # Initialize scoring variables
    score = 0
    anti_score = 0
    relevant_window = (
        50  # Number of characters around a keyword to check for irrelevant content
    )

    # Prepare containers for processing keywords and positions
    found_keywords = []
    found_anti_keywords = []

    # Function to determine if a segment of text contains any irrelevant phrases
    def contains_irrelevant(phrases, segment):
        return any(
            re.search(r"\b" + re.escape(phrase) + r"\b", segment, re.IGNORECASE)
            for phrase in phrases
        )

    # Extract relevant keywords and their positions, using word boundaries to avoid partial matches
    for keyword in self.keywords:
        for match in re.finditer(
            r"\b" + re.escape(keyword) + r"\b", text, re.IGNORECASE
        ):
            start, end = match.start(), match.end()
            # Check for nearby irrelevant content within a defined window
            if not contains_irrelevant(
                self.irrelevant_for_keywords,
                text[max(0, start - relevant_window) : end + relevant_window],
            ):
                found_keywords.append((keyword, start, end))

    # Calculate relevant score based on unique keyword positions
    processed_positions = set()
    for keyword, start, end in found_keywords:
        # Only count keywords that are not overlapping with already processed positions
        if not any(pos in processed_positions for pos in range(start, end)):
            # Assign weights dynamically based on keyword length or type
            keyword_weight = len(
                keyword.split()
            )  # Example: Longer phrases have more weight
            score += keyword_weight
            processed_positions.update(range(start, end))

    # Extract anti-keywords and their positions
    for anti_keyword in self.anti_keywords:
        for match in re.finditer(
            r"\b" + re.escape(anti_keyword) + r"\b", text, re.IGNORECASE
        ):
            start, end = match.start(), match.end()
            # Check for nearby relevant content within a defined window
            if not contains_irrelevant(
                self.irrelevant_for_anti_keywords,
                text[max(0, start - relevant_window) : end + relevant_window],
            ):
                found_anti_keywords.append((anti_keyword, start, end))

    # Calculate anti-relevant score based on unique anti-keyword positions
    processed_anti_positions = set()
    for anti_keyword, start, end in found_anti_keywords:
        # Avoid counting anti-keywords that overlap with processed relevant keywords
        if not any(pos in processed_positions for pos in range(start, end)):
            # Assign weights dynamically to anti-keywords, possibly lower than for relevant keywords
            anti_score += 1  # Simple weight for anti-keywords
            processed_anti_positions.update(range(start, end))

    # Calculate the final score and adjust based on the presence of both relevant and irrelevant keywords
    final_score = score - anti_score

    # Return the final score and a list of found relevant keywords for reporting
    return (
        final_score,
        list(set([kw for kw, _, _ in found_keywords])),
        list(set([kw for kw, _, _ in found_anti_keywords])),
    )
