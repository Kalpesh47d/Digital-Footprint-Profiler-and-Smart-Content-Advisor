
import streamlit as st
import pandas as pd
import json
from kafka import KafkaConsumer
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import os
from dotenv import load_dotenv
from collections import deque

# Load environment variables
load_dotenv()

# --- Page Configuration ---
st.set_page_config(
    page_title="Real-Time Social Media Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- Kafka Configuration ---
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "testprac")

# --- Streamlit Session State Initialization ---
if 'data_df' not in st.session_state:
    st.session_state.data_df = pd.DataFrame(columns=[
        'source', 'user', 'content', 'metric_1', 'metric_2', 'timestamp'
    ])
if 'recent_data' not in st.session_state:
    st.session_state.recent_data = deque(maxlen=10)

# --- Kafka Consumer ---
@st.cache_resource
def get_kafka_consumer(topic):
    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=KAFKA_BROKER,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='latest', # Start from the latest message
            enable_auto_commit=True
        )
        st.success("Successfully connected to Kafka topic.")
        return consumer
    except Exception as e:
        st.error(f"Failed to connect to Kafka: {e}")
        return None

consumer = get_kafka_consumer(KAFKA_TOPIC)

# --- Data Normalization ---
def normalize_data(msg):
    """Normalizes data from different sources into a standard format."""
    source = msg.get('source', 'unknown')
    if source == 'instagram':
        return {
            'source': 'Instagram',
            'user': msg.get('user'),
            'content': msg.get('description'),
            'metric_1': int(str(msg.get('likes', 0)).replace(',', '')),
            'metric_2': msg.get('comment_count', 0),
            'timestamp': pd.to_datetime(msg.get('timestamp'))
        }
    elif source == 'reddit':
        return {
            'source': 'Reddit',
            'user': msg.get('author'),
            'content': msg.get('title') or msg.get('body'),
            'metric_1': msg.get('score', 0),
            'metric_2': msg.get('num_comments', 0),
            'timestamp': pd.to_datetime(msg.get('timestamp'))
        }
    elif source == 'nitter':
        return {
            'source': 'Twitter',
            'user': msg.get('username'),
            'content': msg.get('content'),
            'metric_1': msg.get('likes', 0),
            'metric_2': msg.get('retweets', 0),
            'timestamp': pd.to_datetime(msg.get('timestamp'), errors='coerce')
        }
    return None


# --- UI Layout ---
st.title("📊 Real-Time Social Media Activity")
st.markdown("This dashboard streams live data from Kafka. Run the scraper scripts to see updates.")

# Placeholders for live updates
placeholder = st.empty()

# --- Main Loop ---
if consumer:
    st.sidebar.header("Controls")
    if st.sidebar.button("Clear Data"):
        st.session_state.data_df = pd.DataFrame(columns=['source', 'user', 'content', 'metric_1', 'metric_2', 'timestamp'])
        st.session_state.recent_data.clear()
        st.toast("Cleared all data!")

    st.sidebar.header("Live Feed Status")
    status_placeholder = st.sidebar.empty()

    for message in consumer:
        normalized_post = normalize_data(message.value)
        if normalized_post:
            # Append to deque for recent activity log
            st.session_state.recent_data.appendleft(normalized_post)

            # Append to main dataframe
            new_df = pd.DataFrame([normalized_post])
            st.session_state.data_df = pd.concat([st.session_state.data_df, new_df], ignore_index=True)
            
            df = st.session_state.data_df
            
            with placeholder.container():
                # --- KPIs ---
                total_posts = len(df)
                avg_metric_1 = df['metric_1'].mean()
                avg_metric_2 = df['metric_2'].mean()

                kpi1, kpi2, kpi3 = st.columns(3)
                kpi1.metric(label="Total Posts Received", value=f"{total_posts} 📈")
                kpi2.metric(label="Avg. Primary Metric (Likes/Score)", value=f"{avg_metric_1:.2f} 👍")
                kpi3.metric(label="Avg. Secondary Metric (Comments/Retweets)", value=f"{avg_metric_2:.2f} 💬")
                
                st.markdown("---")

                # --- Charts ---
                col1, col2 = st.columns([2, 1])
                with col1:
                    st.subheader("Posts Over Time by Source")
                    if not df.empty:
                        time_df = df.set_index('timestamp').groupby('source').resample('5S').size().reset_index(name='count')
                        fig_time = go.Figure()
                        for source in time_df['source'].unique():
                            source_df = time_df[time_df['source'] == source]
                            fig_time.add_trace(go.Scatter(x=source_df['timestamp'], y=source_df['count'], mode='lines+markers', name=source))
                        fig_time.update_layout(title="Post Frequency (5-second intervals)", paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0.2)')
                        st.plotly_chart(fig_time, use_container_width=True)

                with col2:
                    st.subheader("Post Distribution by Source")
                    source_counts = df['source'].value_counts()
                    fig_pie = go.Figure(data=[go.Pie(labels=source_counts.index, values=source_counts.values, hole=.4)])
                    fig_pie.update_layout(title="Source Distribution", paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)')
                    st.plotly_chart(fig_pie, use_container_width=True)

                # --- Recent Activity Log ---
                st.subheader("Recent Activity Stream")
                st.dataframe(pd.DataFrame(list(st.session_state.recent_data)), use_container_width=True)

            status_placeholder.success(f"Last update: {pd.Timestamp.now().strftime('%H:%M:%S')}")
else:
    st.warning("Kafka consumer is not available. Please check your connection settings in the .env file.")
