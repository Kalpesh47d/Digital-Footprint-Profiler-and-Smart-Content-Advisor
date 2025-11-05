
import time
import json
import random
from datetime import datetime, timezone
from pymongo import MongoClient
import requests
import os
import sys
from dotenv import load_dotenv
from kafka import KafkaProducer

# Load environment variables
load_dotenv()

# --- Kafka Setup ---
KAFKA_BROKER = os.getenv("KAFKA_BROKER")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
producer = None
if KAFKA_BROKER and KAFKA_TOPIC:
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8')
        )
        print("Successfully connected to Kafka.")
    except Exception as e:
        print(f"Failed to connect to Kafka: {e}")
else:
    print("Kafka broker/topic not found in .env file. Kafka producing will be disabled.")


# --- MongoDB Setup ---
MONGO_URI = os.getenv("MONGO_URI")
client = None
collection = None
if MONGO_URI:
    try:
        client = MongoClient(MONGO_URI)
        db = client['reddit_user_data']
        collection = db['user_activity']
        print("Successfully connected to MongoDB.")
    except Exception as e:
        print(f"Failed to connect to MongoDB: {e}")
        client = None
else:
    print("MONGO_URI not found in .env file. MongoDB operations will be disabled.")


# --- Headers ---
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

def fetch_reddit_json(url, max_items=80):
    items = []
    after = None
    print(f"Fetching data from JSON API endpoint: {url}")
    try:
        while len(items) < max_items:
            full_url = f"{url}.json?limit=100" + (f"&after={after}" if after else "")
            
            try:
                r = requests.get(full_url, headers=HEADERS, timeout=15)
                r.raise_for_status()
            except requests.exceptions.RequestException as e:
                print(f"Network error while fetching {full_url}: {e}")
                break

            data = r.json()
            children = data.get("data", {}).get("children", [])
            if not children:
                print("No new items found. Ending fetch.")
                break
            
            items.extend(children)
            after = data.get("data", {}).get("after")
            print(f"Fetched {len(items)} items so far...")
            
            if not after:
                print("Reached end of data. No more pages.")
                break
            
            time.sleep(random.uniform(1.5, 3.5))

    except Exception as e:
        print(f"An unexpected error occurred in fetch_reddit_json: {e}")
    return items[:max_items]


def parse_and_produce(json_items, item_type):
    """Parses data and sends it to Kafka, returning a list for DB insertion."""
    parsed_data = []
    for item in json_items:
        data = item.get("data", {})
        created_utc = data.get("created_utc")
        
        if item_type == 'post':
            record = {
                "source": "reddit",
                "type": "submission",
                "author": data.get("author", "N/A"),
                "title": data.get("title"),
                "body": data.get("selftext", data.get("url")),
                "subreddit": data.get("subreddit", "N/A"),
                "score": data.get("score", 0),
                "num_comments": data.get("num_comments", 0),
                "permalink": f"https://reddit.com{data.get('permalink', '')}",
                "timestamp": datetime.fromtimestamp(created_utc, timezone.utc).isoformat() if created_utc else None
            }
        else: # comment
            record = {
                "source": "reddit",
                "type": "comment",
                "author": data.get("author", "N/A"),
                "body": data.get("body", "").strip(),
                "subreddit": data.get("subreddit", "N/A"),
                "score": data.get("score", 0),
                "permalink": f"https://reddit.com{data.get('permalink', '')}",
                "timestamp": datetime.fromtimestamp(created_utc, timezone.utc).isoformat() if created_utc else None
            }
        
        parsed_data.append(record)
        if producer:
            producer.send(KAFKA_TOPIC, record)
            time.sleep(random.uniform(0.1, 0.5))

    if producer:
        producer.flush()
        print(f"Produced {len(parsed_data)} {item_type}s to Kafka topic '{KAFKA_TOPIC}'.")
        
    return parsed_data


def save_to_mongodb(data):
    if collection is None:
        print("MongoDB collection not available. Skipping save.")
        return
    if data:
        try:
            collection.insert_many(data, ordered=False)
            print(f"Inserted {len(data)} documents into MongoDB.")
        except Exception as e:
            print(f"Error saving to MongoDB: {e}")

def scrape_user(username, max_posts=40, max_comments=40):
    print(f"--- Starting scrape for Reddit user: {username} ---")
    # Scrape posts
    posts_json = fetch_reddit_json(f"https://www.reddit.com/user/{username}/submitted", max_posts)
    if posts_json:
        posts_for_db = parse_and_produce(posts_json, 'post')
        save_to_mongodb(posts_for_db)

    # Scrape comments
    comments_json = fetch_reddit_json(f"https://www.reddit.com/user/{username}/comments", max_comments)
    if comments_json:
        comments_for_db = parse_and_produce(comments_json, 'comment')
        save_to_mongodb(comments_for_db)
    print(f"--- Finished scrape for Reddit user: {username} ---")

# --- Run ---
if __name__ == '__main__':
    if len(sys.argv) > 1:
        username_to_scrape = sys.argv[1]
    else:
        username_to_scrape = "spez" # Default user if no argument is provided
    
    scrape_user(username_to_scrape)
    
    if producer:
        producer.close()
