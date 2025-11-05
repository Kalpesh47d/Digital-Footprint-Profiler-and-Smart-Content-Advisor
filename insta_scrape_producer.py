import time
import json
import os
from pymongo import MongoClient
from kafka import KafkaProducer
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# ===== CONFIG =====
USERNAME = os.getenv("INSTA_USERNAME")
PASSWORD = os.getenv("INSTA_PASSWORD")
MONGO_URI = os.getenv("MONGO_URI")
KAFKA_BROKER = os.getenv("KAFKA_BROKER")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")

if not all([USERNAME, PASSWORD, MONGO_URI, KAFKA_BROKER, KAFKA_TOPIC]):
    print("Error: Missing one or more essential environment variables.")
    exit()

PROFILE_URL = f'https://www.instagram.com/{USERNAME}/'
SCROLL_PAUSE_TIME = 5
MAX_SCROLL_ATTEMPTS = 15

# ===== MONGO CONFIG =====
MONGO_DB = "instagram_data"
MONGO_COLLECTION = "posts"

# Initialize MongoDB & Kafka connections
mongo_client = MongoClient(MONGO_URI)
mongo_collection = mongo_client[MONGO_DB][MONGO_COLLECTION]

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8')
)

# Setup Selenium WebDriver
options = Options()
options.add_argument('--start-maximized')
options.add_argument("--disable-blink-features=AutomationControlled")

# Use webdriver-manager to automatically handle the driver
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
wait = WebDriverWait(driver, 15)

# === LOGIN ===
driver.get("https://www.instagram.com/accounts/login/")
wait.until(EC.presence_of_element_located((By.NAME, 'username'))).send_keys(USERNAME)
wait.until(EC.presence_of_element_located((By.NAME, 'password'))).send_keys(PASSWORD)
driver.find_element(By.NAME, "password").send_keys(Keys.RETURN)

# Handle pop-ups like "Save Info" or notifications
try:
    # Wait for the "Not Now" button for saving login info
    wait.until(EC.element_to_be_clickable((By.XPATH, "//div[contains(text(),'Not Now')]" ))).click()
    # Wait for the "Not Now" button for turning on notifications
    wait.until(EC.element_to_be_clickable((By.XPATH, "//button[contains(text(),'Not Now')]" ))).click()
except Exception as e:
    print(f"Could not find or click pop-up buttons, continuing... Error: {e}")


# Go to own profile
driver.get(PROFILE_URL)
wait.until(EC.presence_of_element_located((By.XPATH, "//a[contains(@href, '/p/')]" )))


# Extract profile description
try:
    bio_elem = wait.until(EC.presence_of_element_located((By.XPATH, "//h1/../div/span")))
    profile_description = bio_elem.text.replace('\n', ' ').strip()
except Exception as e:
    profile_description = "N/A"
    print(f"Could not extract bio: {e}")


print("Profile Description:", profile_description)

# Collect post URLs, skipping those already in MongoDB
post_links = set()
last_height = driver.execute_script("return document.body.scrollHeight")
scroll_attempts = 0

while scroll_attempts < MAX_SCROLL_ATTEMPTS:
    anchors = driver.find_elements(By.XPATH, "//a[contains(@href, '/p/')]" )
    new_links_found = False
    for a in anchors:
        href = a.get_attribute('href')
        if href and href not in post_links:
            if not mongo_collection.find_one({"url": href}):
                post_links.add(href)
                new_links_found = True

    if not new_links_found and len(anchors) > 0:
        print("No new post links found on this scroll, checking for end of page.")
        scroll_attempts += 1
    else:
        scroll_attempts = 0 # Reset if new links are found

    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    time.sleep(SCROLL_PAUSE_TIME) # Keep sleep here as we need to wait for page to load
    new_height = driver.execute_script("return document.body.scrollHeight")
    if new_height == last_height:
        print("Reached end of scroll.")
        break
    last_height = new_height


if not post_links:
    print("No new posts found. Exiting.")
    driver.quit()
    producer.close()
    exit()

print(f"Found {len(post_links)} new posts. Scraping now...")

# Process each new post
for url in sorted(list(post_links)):
    driver.get(url)
    try:
        wait.until(EC.presence_of_element_located((By.XPATH, "//time")))
    except Exception as e:
        print(f"Failed to load post page {url}, skipping. Error: {e}")
        continue


    # Likes
    try:
        # This selector is fragile. Instagram often changes class names.
        likes_xpath = "//section//span[contains(text(), 'likes') or contains(text(), 'like')]/span"
        likes = wait.until(EC.presence_of_element_located((By.XPATH, likes_xpath))).text.strip()
    except Exception:
         likes = "N/A"

    # Description (caption + links)
    try:
        desc_elem = wait.until(EC.presence_of_element_located((By.XPATH, "//h1")))
        description = desc_elem.text.strip()
    except Exception:
        description = "No description"

    # Comments count
    try:
        # This is not a reliable way to get all comments. It's better to get the count if available.
        comment_count_text = driver.find_element(By.XPATH, "//h1/../../div[last()]/a/span").text
        comment_count = int(comment_count_text.split()[0].replace(',', ''))
    except Exception:
        comment_count = 0

    # Timestamp
    try:
        timestamp = driver.find_element(By.XPATH, "//time").get_attribute("datetime")
    except Exception:
        timestamp = "N/A"

    post_data = {
        "user": USERNAME,
        "url": url,
        "likes": likes,
        "description": description,
        "comment_count": comment_count,
        "timestamp": timestamp,
        "profile_description": profile_description
    }

    print(f"Sending post to Kafka: {url}")
    producer.send(KAFKA_TOPIC, post_data)
    time.sleep(1) # Small delay between kafka sends

driver.quit()
producer.close()
print("Done — all new posts sent.")