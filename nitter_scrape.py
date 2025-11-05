
import random
import requests
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException
from dataclasses import dataclass, asdict
from typing import List, Optional
import logging
import time
from pymongo import MongoClient
import json
import os
import sys
from datetime import datetime
from dotenv import load_dotenv
from webdriver_manager.chrome import ChromeDriverManager
from kafka import KafkaProducer

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

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
        logger.info("Successfully connected to Kafka.")
    except Exception as e:
        logger.error(f"Failed to connect to Kafka: {e}")
else:
    logger.warning("Kafka broker/topic not found in .env file. Kafka producing will be disabled.")

# --- MongoDB Setup ---
MONGO_URI = os.getenv("MONGO_URI")
client = None
collection = None
if MONGO_URI:
    try:
        client = MongoClient(MONGO_URI)
        db = client['twitter_data']
        collection = db['tweets_nitter']
        logger.info("Successfully connected to MongoDB")
    except Exception as e:
        logger.error(f"Failed to connect to MongoDB: {e}")
        client = None
else:
    logger.warning("MONGO_URI not found in .env file. MongoDB connection will be skipped.")


@dataclass
class TweetData:
    """Data class for tweet information"""
    source: str
    url: str
    username: str
    timestamp: str
    content: str
    comments: int
    retweets: int
    quotes: int
    likes: int

class OptimizedTwitterScraper:
    def __init__(self, headless: bool = True):
        self.headless = headless
        self.driver = None
        self.wait = None
        self.nitter_instances = [
            'https://xcancel.com', 'https://nitter.poast.org', 
            'https://nitter.privacyredirect.com', 'https://nitter.tiekoetter.com'
        ]
        self.current_instance = 0
        
    def setup_driver(self) -> None:
        options = Options()
        if self.headless:
            options.add_argument('--headless=new')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-blink-features=AutomationControlled')
        try:
            service = Service(ChromeDriverManager().install())
            self.driver = webdriver.Chrome(service=service, options=options)
            self.wait = WebDriverWait(self.driver, 10)
            logger.info("Chrome driver setup completed")
        except WebDriverException as e:
            logger.error(f"Failed to setup driver: {e}")
            raise

    def get_available_instance(self) -> str:
        for _ in range(len(self.nitter_instances)):
            instance = self.nitter_instances[self.current_instance]
            self.current_instance = (self.current_instance + 1) % len(self.nitter_instances)
            try:
                response = requests.head(instance, timeout=5)
                if response.status_code == 200:
                    logger.info(f"Using Nitter instance: {instance}")
                    return instance
            except requests.RequestException:
                continue
        logger.warning("No available Nitter instances found, using default")
        return self.nitter_instances[0]


    def extract_tweet_data(self, tweet_element) -> Optional[TweetData]:
        try:
            tweet_link = tweet_element.find_element(By.CSS_SELECTOR, ".tweet-link").get_attribute("href")
            content_text = tweet_element.find_element(By.CSS_SELECTOR, ".tweet-content").text.strip()
            username = tweet_element.find_element(By.CSS_SELECTOR, ".tweet-name-row a.fullname").get_attribute("title")
            timestamp_str = tweet_element.find_element(By.CSS_SELECTOR, ".tweet-name-row .tweet-date").get_attribute("title")
            timestamp = datetime.strptime(timestamp_str, '%b %d, %Y · %I:%M %p %Z').isoformat()

            comments, retweets, quotes, likes = 0, 0, 0, 0
            stats = tweet_element.find_elements(By.CSS_SELECTOR, ".tweet-stats .tweet-stat")
            if len(stats) >= 4:
                comments = int(stats[0].text.strip().replace(',', '') or 0)
                retweets = int(stats[1].text.strip().replace(',', '') or 0)
                quotes = int(stats[2].text.strip().replace(',', '') or 0)
                likes = int(stats[3].text.strip().replace(',', '') or 0)

            return TweetData(
                source="nitter",
                url=tweet_link or "", username=username, timestamp=timestamp,
                content=content_text, comments=comments, retweets=retweets,
                quotes=quotes, likes=likes
            )
        except (NoSuchElementException, ValueError, AttributeError) as e:
            logger.error(f"Error extracting tweet data: {e}")
            return None

    def scrape_tweets(self, username: str, max_tweets: int = 50) -> List[TweetData]:
        """Main scraping method that now produces to Kafka."""
        tweets_data = []
        logger.info(f"--- Starting scrape for Twitter user: {username} ---")
        try:
            self.setup_driver()
            base_url = f'{self.get_available_instance()}/{username}'
            logger.info(f"Starting to scrape tweets from: {base_url}")
            self.driver.get(base_url)
            
            last_height = self.driver.execute_script("return document.body.scrollHeight")
            
            while len(tweets_data) < max_tweets:
                try:
                    self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, ".timeline-item")))
                except TimeoutException:
                    logger.info("No more tweets found or page timed out.")
                    break

                tweets_on_page = self.driver.find_elements(By.CSS_SELECTOR, ".timeline-item")
                
                new_tweets_found = False
                for tweet_el in tweets_on_page:
                    tweet_url = tweet_el.find_element(By.CSS_SELECTOR, ".tweet-link").get_attribute("href")
                    if any(t.url == tweet_url for t in tweets_data):
                        continue

                    if len(tweets_data) >= max_tweets:
                        break
                        
                    tweet_data = self.extract_tweet_data(tweet_el)
                    if tweet_data:
                        new_tweets_found = True
                        tweets_data.append(tweet_data)
                        
                        if producer:
                            producer.send(KAFKA_TOPIC, asdict(tweet_data))
                        
                        time.sleep(random.uniform(0.2, 0.6))

                logger.info(f"Scraped {len(tweets_data)} tweets so far...")

                if len(tweets_data) >= max_tweets or not new_tweets_found:
                    break

                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(3)
                
                new_height = self.driver.execute_script("return document.body.scrollHeight")
                if new_height == last_height:
                    logger.info("Reached end of page.")
                    break
                last_height = new_height

            if tweets_data:
                logger.info(f"Successfully scraped and produced {len(tweets_data)} tweets.")
                self.save_to_mongodb(tweets_data)
            else:
                logger.warning("No new tweets were scraped.")
                
        except Exception as e:
            logger.error(f"An error occurred during scraping: {e}")
        finally:
            if self.driver:
                self.driver.quit()
            if producer:
                producer.flush()
                logger.info("Flushed Kafka producer.")
        
        logger.info(f"--- Finished scrape for Twitter user: {username} ---")
        return tweets_data

    def save_to_mongodb(self, tweets_data: List[TweetData]):
        if client and collection is not None and tweets_data:
            try:
                collection.insert_many([asdict(t) for t in tweets_data], ordered=False)
                logger.info(f"Inserted {len(tweets_data)} documents into MongoDB.")
            except Exception as e:
                logger.error(f"Error saving to MongoDB: {e}")

# --- Run ---
if __name__ == "__main__":
    if len(sys.argv) > 1:
        username_to_scrape = sys.argv[1]
    else:
        username_to_scrape = "elonmusk" # Default user

    MAX_TWEETS = 30
    
    scraper = OptimizedTwitterScraper(headless=True)
    scraper.scrape_tweets(username=username_to_scrape, max_tweets=MAX_TWEETS)
    
    if producer:
        producer.close()
        logger.info("Kafka producer closed.")
