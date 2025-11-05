from kafka import KafkaConsumer
from pymongo import MongoClient, errors
import json

# === MONGO SETUP ===
client = MongoClient("mongodb://localhost:27017/")
db = client["instagram_data"]
collection = db["posts"]

# === KAFKA SETUP ===
consumer = KafkaConsumer(
    'testprac',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True
)

print("✅ Kafka Consumer started. Listening for messages...")

for message in consumer:
    post_data = message.value
    post_url = post_data.get("Post URL") or post_data.get("url")

    if not post_url:
        print("❌ Skipping message with no 'Post URL'.")
        continue

    # Check for duplication
    if collection.find_one({'Post URL': post_url}):
        print(f"⚠️ Already exists in MongoDB: {post_url}")
        continue

    try:
        collection.insert_one(post_data)
        print(f"✅ Inserted: {post_url}")
    except errors.DuplicateKeyError:
        print(f"⚠️ Duplicate key error on insert: {post_url}")
    except Exception as e:
        print(f"❌ Failed to insert {post_url} — {str(e)}")
