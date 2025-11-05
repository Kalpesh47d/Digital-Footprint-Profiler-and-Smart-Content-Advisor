import streamlit as st
import pandas as pd
from pymongo import MongoClient
import subprocess
import sys
import os
import time
from datetime import datetime
from dotenv import load_dotenv
import analysis

# Load environment variables
load_dotenv()

# --- Page Configuration ---
st.set_page_config(
    page_title="Live User Monitor",
    page_icon="📡",
    layout="wide"
)

# --- MongoDB Connection ---
MONGO_URI = os.getenv("MONGO_URI")
if not MONGO_URI:
    st.error("MONGO_URI not found in .env file. Cannot connect to the database.")
    st.stop()

@st.cache_resource
def get_mongo_client():
    try:
        client = MongoClient(MONGO_URI)
        # Test connection
        client.admin.command('ping')
        return client
    except Exception as e:
        st.error(f"Failed to connect to MongoDB: {e}")
        return None

client = get_mongo_client()
if not client:
    st.stop()

# --- Session State Initialization ---
if 'last_fetch_time' not in st.session_state:
    st.session_state.last_fetch_time = None
if 'data_df' not in st.session_state:
    st.session_state.data_df = pd.DataFrame()
if 'username' not in st.session_state:
    st.session_state.username = "spez"
if 'platform' not in st.session_state:
    st.session_state.platform = "Reddit"
if 'is_running' not in st.session_state:
    st.session_state.is_running = False

# --- Functions ---
def run_scraper(platform, username):
    """Runs the appropriate scraper script as a subprocess."""
    scraper_script = ""
    if platform == "Reddit":
        scraper_script = "Reddit_scrape.py"
    elif platform == "Twitter (via Nitter)":
        scraper_script = "nitter_scrape.py"
    
    if not scraper_script:
        st.error("Invalid platform selected.")
        return False, "Invalid platform", ""

    try:
        process = subprocess.run(
            [sys.executable, scraper_script, username],
            capture_output=True,
            text=True,
            check=True,
            timeout=300
        )
        return True, process.stdout, process.stderr
    except subprocess.CalledProcessError as e:
        return False, e.stdout, e.stderr
    except subprocess.TimeoutExpired:
        return False, "Scraper timed out after 5 minutes.", ""
    except FileNotFoundError:
        return False, f"Scraper script '{scraper_script}' not found.", ""

def fetch_data_from_db(platform, username):
    """Fetches the latest data for a user from MongoDB."""
    db_name, collection_name, user_field = "", "", ""
    if platform == "Reddit":
        db_name, collection_name, user_field = "reddit_user_data", "user_activity", "author"
    elif platform == "Twitter (via Nitter)":
        db_name, collection_name, user_field = "twitter_data", "tweets_nitter", "username"

    db = client[db_name]
    collection = db[collection_name]
    
    query = {user_field: username}
    user_data = list(collection.find(query))
    
    if not user_data:
        return pd.DataFrame()
    
    df = pd.DataFrame(user_data)
    if '_id' in df.columns:
        df = df.drop(columns=['_id'])
    
    # Rename columns for consistency
    if platform == "Reddit":
        df = df.rename(columns={'body': 'content'})
    else: # Twitter
        df = df.rename(columns={'content': 'text', 'url': 'tweet_link'})
        
    return df

def start_analysis():
    """Main function to trigger scraping and data fetching."""
    st.session_state.is_running = True
    st.session_state.data_df = pd.DataFrame() # Clear old data

    # 1. Run Scraper
    with st.status(f"Scraping {st.session_state.platform} for user '{st.session_state.username}'...", expanded=True) as status:
        st.write("Executing scraper script...")
        success, stdout, stderr = run_scraper(st.session_state.platform, st.session_state.username)
        
        if success:
            status.update(label="Scraping successful! Fetching data...", state="running")
            st.text_area("Scraper Log", stdout, height=100)
        else:
            status.update(label="Scraping failed!", state="error")
            st.text_area("Scraper STDOUT", stdout, height=100)
            st.text_area("Scraper STDERR", stderr, height=100)
            st.session_state.is_running = False
            return

        # 2. Fetch Data
        st.write("Querying database for new records...")
        df = fetch_data_from_db(st.session_state.platform, st.session_state.username)
        
        if df.empty:
            status.update(label=f"No data found for '{st.session_state.username}'.", state="error")
            st.session_state.is_running = False
            return
        
        st.session_state.data_df = df
        st.session_state.last_fetch_time = datetime.now()
        status.update(label="Analysis complete!", state="complete")

    st.session_state.is_running = False
    st.rerun()


# --- UI Layout ---
st.title("📡 Live Social Media User Monitor")

# --- Sidebar for Controls ---
with st.sidebar:
    st.header("Controls")
    
    st.session_state.platform = st.selectbox(
        "Select Platform",
        ("Reddit", "Twitter (via Nitter)"),
        index=["Reddit", "Twitter (via Nitter)"].index(st.session_state.platform)
    )
    
    st.session_state.username = st.text_input("Enter Username", st.session_state.username)

    if st.button("Start/Update Analysis", type="primary", use_container_width=True, disabled=st.session_state.is_running):
        if not st.session_state.username:
            st.warning("Please enter a username.")
        else:
            start_analysis()

    if st.session_state.last_fetch_time:
        st.success(f"Data for '{st.session_state.username}' loaded.")
        st.info(f"Last updated: {st.session_state.last_fetch_time.strftime('%H:%M:%S')}")
    
    st.markdown("---")
    refresh_interval = st.slider("Auto-refresh interval (minutes)", 1, 60, 5)


# --- Main Dashboard Area ---
if not st.session_state.data_df.empty:
    analysis_platform = 'reddit' if st.session_state.platform == 'Reddit' else 'twitter'
    
    # This is the main call to the refactored analysis module
    analysis.run_analysis_and_generate_dashboard(st.session_state.data_df, analysis_platform)
    
    # Auto-refresh logic
    time.sleep(refresh_interval * 60)
    st.rerun()

elif st.session_state.is_running:
    st.info("Analysis is in progress. Please wait...")

else:
    st.info("Click 'Start/Update Analysis' in the sidebar to begin.")