import json
import praw
import logging
import time
from prawcore.exceptions import RequestException

# Initialize logging for easy tracking
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
fs = open("config/reddit.json", "r")
keys = json.loads(fs.read())
# Initialize Reddit instance
reddit = praw.Reddit(
    client_id=keys["client_id"],
    client_secret=keys["client_secret"],
    user_agent=keys["user_agent"],
)


def search_by_domain(domain, subreddit_name="transgenderUK", batch_size=100):
    """
    Searches for posts linking to a specific domain in a subreddit.

    Parameters:
    - domain (str): The domain to search for in submissions.
    - subreddit_name (str): The name of the subreddit to search within.
    - batch_size (int): Number of results per batch (max: 100).

    Returns:
    - all_results (list): List of results matching the domain.
    """
    try:
        print("search_by_domain")
        all_results = []
        output = {}
        after = None  # Track the last submission ID
        seen = set()

        # Loop to retrieve posts linking to the specified domain
        while True:
            results = reddit.subreddit(subreddit_name).search(
                f"site:{domain}", sort="new", limit=batch_size, params={"after": after}
            )
            print(results)
            results = list(results)  # Convert generator to list
            # Break if no more results
            if not results:
                break

            # Append results to the list
            all_results.extend(results)

            # Update `after` to the ID of the last item in the batch
            after = results[-1].id

            # Process each result
            for submission in results:
                if submission.id in seen:
                    continue
                seen.add(submission.id)
                output[submission.url] = {
                    "subreddit": submission.subreddit.display_name,
                    "flair": submission.link_flair_text,
                    "title": submission.title,
                    "score": submission.score,
                    "created": submission.created_utc,
                    "url": submission.url,
                }
                print(output[submission.url])

            # Optional delay to avoid rate-limiting
            time.sleep(1)

        print(f"Total results retrieved: {len(all_results)}")
        return all_results

    except RequestException as e:
        logging.error(f"An error occurred: {e}")
        return []


# def search_by_domain_full(reddit, domain, keyword="transgender", batch_size=100):
#     """
#     Searches for posts with a specific keyword linking to a domain.

#     Parameters:
#     - reddit (praw.Reddit): An authenticated Reddit instance.
#     - domain (str): The domain to search for in submissions.
#     - keyword (str): Keyword to include in the search query.
#     - batch_size (int): Number of results per batch (max: 100).

#     Returns:
#     - output (dict): Dictionary of results matching the keyword and domain.
#     """
#     try:
#         output = {}
#         query = f"{keyword} site:{domain}"
#         reddit.read_only = True  # Read-only mode to avoid accidental changes
#         # Use the listing generator for pagination
#         results = reddit.subreddit("all").search(query, sort="new", limit=None)

#         for submission in results:
#             output[submission.id] = {
#                 "subreddit": submission.subreddit.display_name,
#                 "flair": submission.link_flair_text,
#                 "title": submission.title,
#                 "score": submission.score,
#                 "created": submission.created_utc,
#                 "url": submission.url,
#             }

#             # Optional: Print progress
#             print(f"Retrieved submission ID: {submission.id}")

#             # Optional delay to respect Reddit's API rate limits
#             time.sleep(1 / batch_size)

#         print(f"Total results retrieved: {len(output)}")
#         return output

#     except Exception as e:
#         logging.error(f"An error occurred: {e}")
#         return {}


all_results = search_by_domain("bbc.co.uk")

# Example usage
# all_results = search_subreddit("bbc.co.uk/news")
