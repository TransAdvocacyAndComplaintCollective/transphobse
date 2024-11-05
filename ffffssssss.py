import time
import praw
import logging
import csv
import re
import os
from typing import Optional
from urllib.parse import urlparse

def setup_logger(log_to_file: bool):
    """
    Sets up the logger for detailed logging.
    """
    logger = logging.getLogger("RedditLinkFetcher")

    # Avoid adding multiple handlers if the logger is already configured
    if not logger.hasHandlers():
        logger.setLevel(logging.DEBUG)
        handler = logging.FileHandler("reddit_link_posts.log") if log_to_file else logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    
    return logger

def initialize_csv(output_csv: str):
    """
    Initializes the CSV file and returns the writer object.
    """
    file_exists = os.path.isfile(output_csv) and os.path.getsize(output_csv) > 0
    csv_file = open(output_csv, mode='a', newline='', encoding='utf-8')
    csv_writer = csv.writer(csv_file)
    
    # Write the header only if the file is empty or doesn't exist
    if not file_exists:
        csv_writer.writerow(['Keyword', 'Title', 'Flair', 'URL', 'Domain', 'Subreddit', 'Score'])
    return csv_file, csv_writer

def clean_url(url: str):
    """
    Cleans URLs by removing image extensions and disallowed domains.
    """
    if "i.redd.it" in url or "reddit.com" in url:
        return None
    return re.sub(r'\.(jpeg|png)$', '', url, flags=re.IGNORECASE)

def process_submission(submission, csv_writer, subreddit_name, query, logger):
    """
    Processes a Reddit submission, logs details, and writes to CSV if it's a valid link post.
    """
    if submission.is_self:
        logger.debug(f"Skipping self post with ID: {submission.id}")
        return
    
    flair = submission.link_flair_text or "No flair"
    url = clean_url(submission.url)
    
    if not url:
        logger.debug(f"Skipping post with disallowed URL: {submission.url}")
        return
    
    title = submission.title
    score = submission.score
    domain = urlparse(url).netloc
    csv_writer.writerow([query, title, flair, url, domain, subreddit_name, score])
    logger.info(f"Processed post: {title}, Flair: {flair}, URL: {url}, Domain: {domain}, Score: {score}")

def fetch_link_posts(
    subreddit_name: str, 
    reddit,
    limit: Optional[int] = None,
    log_to_file: bool = False,
    output_csv: str = "reddit_link_posts.csv"
):
    """
    Fetches and logs link posts from a given subreddit and outputs to a CSV.
    """
    logger = setup_logger(log_to_file)
    logger.info("Starting fetch_link_posts function")

    csv_file, csv_writer = initialize_csv(output_csv)

    try:
        logger.info(f"Accessing subreddit: {subreddit_name}")
        subreddit = reddit.subreddit(subreddit_name)
        
        logger.info("Starting to stream new submissions...")
        submissions = subreddit.stream.submissions()
        count = 0

        for submission in submissions:
            if limit and count >= limit:
                break

            process_submission(submission, csv_writer, subreddit_name, "N/A", logger)
            csv_file.flush()  # Ensure data is written to disk
            count += 1

    except praw.exceptions.PRAWException as e:
        logger.error(f"Reddit API error: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        csv_file.close()
        logger.info("Finished fetch_link_posts function")

def search_link_posts(
    subreddit_name: str, 
    reddit,
    query: str = '', 
    limit: Optional[int] = 100,
    time_filter: str = 'all',
    sort: str = 'relevance',
    log_to_file: bool = False,
    output_csv: str = "reddit_link_posts.csv"
):
    """
    Searches and logs link posts from a given subreddit and outputs to a CSV.
    """
    logger = setup_logger(log_to_file)
    logger.info("Starting search_link_posts function")

    csv_file, csv_writer = initialize_csv(output_csv)

    try:
        logger.info(f"Accessing subreddit: {subreddit_name}")
        subreddit = reddit.subreddit(subreddit_name)
        
        logger.info("Performing search...")
        search_results = subreddit.search(query=query, sort=sort, time_filter=time_filter, limit=limit)

        for submission in search_results:
            process_submission(submission, csv_writer, subreddit_name, query, logger)
            csv_file.flush()  # Ensure data is written to disk

    except praw.exceptions.PRAWException as e:
        logger.error(f"Reddit API error: {e}")
        time.sleep(60)  # Wait before retrying in case of rate limits
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        csv_file.close()
        logger.info("Finished search_link_posts function")

# Example usage
client_id = 'L9FI6a_jhLNzZodqsBcdlQ'
client_secret = 'f9Y2XP6vhqtk5AUOKeENCb8lmSK1Xw'
user_agent = 'TransAdvocacyComplaintBot/1.0 (by u/Ok-Departure7346)'

reddit = praw.Reddit(
    client_id=client_id,
    client_secret=client_secret,
    user_agent=user_agent,
    timeout=60
)

# Example usage with keywords
from utils.keywords import KEYWORDS
for keyword in KEYWORDS:
    search_link_posts(
        subreddit_name='transgenderUK',
        reddit=reddit,
        query=keyword,
        log_to_file=False,
        output_csv="data/reddit_link_posts.csv"
    )

# Fetch link posts
fetch_link_posts(
    subreddit_name='transgenderUK',
    reddit=reddit,
    log_to_file=False,
    output_csv="data/reddit_link_posts2.csv"
)
