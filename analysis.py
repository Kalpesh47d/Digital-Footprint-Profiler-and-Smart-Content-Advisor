import streamlit as st
import pandas as pd
import re
import json
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import nltk
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer
import google.generativeai as genai
import os
from dotenv import load_dotenv
from wordcloud import WordCloud
import matplotlib.pyplot as plt

# Load environment variables
load_dotenv()

# ------------------------
# Setup
# ------------------------
# Download NLTK data if not present
try:
    nltk.data.find('corpora/stopwords')
except LookupError:
    st.info("Downloading NLTK's 'stopwords' resource. This is a one-time download.")
    nltk.download('stopwords')

stop_words = set(stopwords.words('english'))
stemmer = PorterStemmer()

# Get Gemini API key from environment variables
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
if not GEMINI_API_KEY:
    st.error("Gemini API key is not set. Please add it to your .env file.")
    st.stop()

genai.configure(api_key=GEMINI_API_KEY)
# ------------------------
# Text Preprocessing
# ------------------------
@st.cache_data
def preprocess(text: str) -> str:
    if not isinstance(text, str):
        return ""
    text = text.lower()
    text = re.sub(r'http\S+|www\S+|\W+', ' ', text)
    tokens = [stemmer.stem(w) for w in text.split() if w not in stop_words and len(w) > 2]
    return " ".join(tokens)

# ------------------------
# LLM Functions (Gemini) with Caching
# ------------------------
def make_gemini_request(prompt, is_json=False, temperature=0.3):
    """Helper function to make requests to Gemini API."""
    model = genai.GenerativeModel('gemini-1.5-flash')
    config = {
        "temperature": temperature,
        "response_mime_type": "application/json" if is_json else "text/plain"
    }
    try:
        response = model.generate_content(prompt, generation_config=genai.types.GenerationConfig(**config))
        content = response.text
        return json.loads(content) if is_json else content
    except json.JSONDecodeError as e:
        st.error(f"Failed to parse JSON response from API: {e}")
        st.error(f"Raw response: {content}")
        return None
    except Exception as e:
        st.error(f"An API error occurred: {e}")
        return None

@st.cache_data
def analyze_sentiment_batch(texts: tuple) -> list:
    """Analyzes a batch of texts for sentiment using Gemini. Caches the results."""
    sentiments = []
    system_prompt = (
        "You are a sentiment analyzer. For the given list of texts, "
        "output ONLY a single valid JSON object with a key 'results'. "
        "The value of 'results' should be a list of JSON objects, one for each text, in the same order. "
        "Each object must have the following keys: "
        '{"sentiment": "positive|negative|neutral", "confidence": float, "summary": "A brief one-sentence summary"}. ' 
        "Do not include any other text, explanations, or markdown formatting."
    )
    
    batch_size = 20 # Process up to 20 texts in a single API call
    
    # Filter out empty texts to avoid sending them to the API
    non_empty_texts = [text for text in texts if text.strip()]
    
    for i in range(0, len(non_empty_texts), batch_size):
        batch = non_empty_texts[i:i+batch_size]
        
        # Create a numbered list of texts for the prompt
        user_prompt = "\n".join([f"{idx+1}. {text}" for idx, text in enumerate(batch)])
        full_prompt = f"{system_prompt}\n\nAnalyze the following texts:\n{user_prompt}"
        
        result = make_gemini_request(full_prompt, is_json=True, temperature=0.1)
        
        if result and isinstance(result, dict) and "results" in result and isinstance(result["results"], list):
            if len(result["results"]) == len(batch):
                sentiments.extend(result["results"])
            else:
                st.warning(f"API returned {len(result['results'])} results for a batch of {len(batch)}. Padding with errors.")
                sentiments.extend(result["results"])
                # Pad with error placeholders if the API returned fewer results than expected
                for _ in range(len(batch) - len(result["results"])):
                     sentiments.append({"sentiment": "neutral", "confidence": 0.0, "summary": "API result mismatch"})
        else:
            # The entire batch failed
            st.error(f"API call for batch failed or returned malformed data.")
            for _ in batch:
                sentiments.append({"sentiment": "neutral", "confidence": 0.0, "summary": "Analysis error in batch"})

    # Map the results back to the original list of texts, re-inserting placeholders for the empty texts
    final_results = []
    sentiment_iter = iter(sentiments)
    for text in texts:
        if text.strip():
            try:
                final_results.append(next(sentiment_iter))
            except StopIteration:
                final_results.append({"sentiment": "neutral", "confidence": 0.0, "summary": "Processing error"})
        else:
            final_results.append({"sentiment": "neutral", "confidence": 0.0, "summary": "Empty content"})
            
    return final_results

@st.cache_data
def generate_overall_summary(df_json: str) -> str:
    """Generates an overall summary of the user's activity. Caches the result."""
    df = pd.read_json(df_json)
    if df.empty or 'engagement_score' not in df.columns or df['engagement_score'].isnull().all():
        return "Not enough data to generate a summary."

    sentiment_dist = df['sentiment'].value_counts(normalize=True).to_dict()
    top_post = df.loc[df['engagement_score'].idxmax()]
    
    prompt = (
        f"Based on the following data, provide a concise, engaging summary of this user's social media presence. "
        f"Mention their overall sentiment, the topic of their most engaging post, and their general tone.\n\n"
        f"Sentiment Distribution: {sentiment_dist}\n"
        f"Most Engaging Post (Summary): {top_post['summary']}\n"
        f"Most Engaging Post Score: {top_post['engagement_score']}"
    )
    
    summary = make_gemini_request(prompt, temperature=0.6)
    return summary or "Could not generate an overall summary due to an API error."

@st.cache_data
def generate_content_suggestions(df_json: str, platform: str) -> dict:
    """Generates content suggestions based on top posts. Caches the result."""
    df = pd.read_json(df_json)
    if df.empty or 'engagement_score' not in df.columns or df['engagement_score'].isnull().all():
        return {"suggestions": [{"title": "No Data", "content": "Not enough data to generate suggestions.", "hashtags": ""}]}

    top_posts = df.nlargest(5, 'engagement_score')['summary'].tolist()
    prompt = (
        f"I am a content creator on {platform}. My most successful posts are summarized as follows:\n{top_posts}\n\n"
        "Based on this, generate 3 new, creative content ideas. For each idea, provide a compelling title, "
        "a short paragraph of content, and a string of relevant hashtags. "
        "Format the output as a single JSON object with a key 'suggestions' which is a list of objects, "
        "each with keys 'title', 'content', and 'hashtags'. NO extra text."
    )
    
    suggestions = make_gemini_request(prompt, is_json=True, temperature=0.7)
    return suggestions or {"suggestions": [{"title": "Error", "content": "Could not generate suggestions.", "hashtags": ""}]}

@st.cache_data
def predict_engagement(text: str) -> dict:
    """Predicts engagement for a given text. Caches the result."""
    prompt = (
        'Analyze the following post text and predict its sentiment and potential engagement level (Low, Medium, High). '
        'Provide a short suggestion to improve it. Output ONLY a valid JSON object with keys: ' 
        '{"sentiment": "positive|negative|neutral", "engagement_level": "Low|Medium|High", "suggestion": "Your suggestion"}.'
    )
    
    prediction = make_gemini_request(prompt, is_json=True)
    return prediction or {"sentiment": "N/A", "engagement_level": "N/A", "suggestion": "Prediction failed."}

# ------------------------
# Plotting Functions
# ------------------------
def plot_sentiment_distribution(df: pd.DataFrame):
    """Creates an interactive pie chart for sentiment distribution."""
    if df.empty or 'sentiment' not in df.columns:
        st.warning("No sentiment data available to plot.")
        return

    sentiment_counts = df['sentiment'].value_counts()
    colors = {'positive': '#2ca02c', 'negative': '#d62728', 'neutral': '#7f7f7f'}
    
    fig = go.Figure(data=[go.Pie(
        labels=sentiment_counts.index,
        values=sentiment_counts.values,
        hoverinfo='label+percent',
        textinfo='label+percent',
        hole=.4,
        marker_colors=[colors.get(key, '#CCCCCC') for key in sentiment_counts.index],
        pull=[0.05] * len(sentiment_counts)
    )])
    fig.update_layout(
        title_text="Sentiment Distribution",
        title_x=0.5,
        legend_title_text='Sentiments',
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        font_color='white'
    )
    st.plotly_chart(fig, use_container_width=True)

def plot_engagement_timeline(df: pd.DataFrame, platform: str):
    """Creates an interactive timeline of engagement metrics."""
    if df.empty or 'timestamp' not in df.columns:
        st.warning("No data available for timeline.")
        return
        
    df_sorted = df.sort_values('timestamp')
    fig = make_subplots(specs=[[{"secondary_y": False}]])
    
    metrics = {
        'twitter': [('Likes', 'likes', '#1DA1F2'), ('Retweets', 'retweets', '#17BF63'), ('Replies', 'replies', '#FFAD1F')],
        'reddit': [('Score', 'score', '#FF4500'), ('Comments', 'num_comments', '#7E53C1')]
    }
    
    for label, col, color in metrics.get(platform, []):
        if col in df_sorted.columns:
            fig.add_trace(go.Scatter(
                x=df_sorted['timestamp'], y=df_sorted[col], name=label,
                mode='lines+markers', marker_color=color, line=dict(width=3)
            ))
        
    fig.update_layout(
        title_text="Engagement Over Time", title_x=0.5,
        xaxis_title="Date", yaxis_title="Count",
        legend_title="Metric",
        paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0.2)',
        font_color='white',
        hovermode="x unified"
    )
    st.plotly_chart(fig, use_container_width=True)

def plot_activity_heatmap(df: pd.DataFrame):
    """Creates a heatmap of user activity by day of week and hour."""
    if df.empty or 'timestamp' not in df.columns:
        st.warning("No timestamp data available for activity heatmap.")
        return

    df['hour'] = df['timestamp'].dt.hour
    df['day_of_week'] = df['timestamp'].dt.day_name()
    
    activity = df.groupby(['day_of_week', 'hour']).size().unstack(fill_value=0)
    days_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    activity = activity.reindex(days_order)
    
    fig = go.Figure(data=go.Heatmap(
        z=activity.values,
        x=activity.columns,
        y=activity.index,
        colorscale='Viridis',
        hoverongaps=False
    ))
    fig.update_layout(
        title='User Activity Heatmap',
        xaxis_nticks=24,
        xaxis_title="Hour of Day",
        yaxis_title="Day of Week",
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        font_color='white'
    )
    st.plotly_chart(fig, use_container_width=True)

def plot_word_cloud(df: pd.DataFrame, platform: str):
    """Generates and displays a word cloud from processed text."""
    content_col = 'processed_text'
    if df.empty or content_col not in df.columns or df[content_col].isnull().all():
        st.warning("No text data available for word cloud.")
        return

    text = " ".join(review for review in df[content_col].dropna())
    if not text.strip():
        st.warning("Content is empty after processing; cannot generate word cloud.")
        return
        
    wordcloud = WordCloud(
        width=800, 
        height=400, 
        background_color='rgba(0,0,0,0)', 
        colormap='viridis',
        mode="RGBA"
    ).generate(text)
    
    fig, ax = plt.subplots()
    ax.imshow(wordcloud, interpolation='bilinear')
    ax.axis('off')
    st.pyplot(fig, use_container_width=True)


def plot_subreddit_activity(df: pd.DataFrame):
    """Bar chart of activity per subreddit."""
    if 'subreddit' not in df.columns:
        return # Silently fail if not a reddit dataframe
    
    subreddit_counts = df['subreddit'].value_counts().nlargest(15)
    fig = go.Figure(go.Bar(
        x=subreddit_counts.index,
        y=subreddit_counts.values,
        marker_color='orange'
    ))
    fig.update_layout(
        title='Top 15 Subreddit Activity',
        xaxis_title='Subreddit',
        yaxis_title='Number of Posts/Comments',
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0.2)',
        font_color='white'
    )
    st.plotly_chart(fig, use_container_width=True)


# ------------------------
# Main Analysis Orchestrator
# ------------------------
def run_analysis_and_generate_dashboard(df: pd.DataFrame, platform: str):
    """Main function to process the dataframe and generate the full dashboard."""
    if df.empty:
        st.warning("The uploaded data is empty. Cannot perform analysis.")
        return df, "No summary available."

    # 1. Preprocess and Analyze Sentiment
    content_col = 'text' if platform == 'twitter' else 'content'
    if content_col not in df.columns:
        st.error(f"Expected column '{content_col}' not found in the data for platform '{platform}'.")
        return df, "Analysis failed."

    df['processed_text'] = df[content_col].apply(preprocess)
    # Pass a tuple to make it hashable for caching
    sentiment_results = analyze_sentiment_batch(tuple(df['processed_text'].tolist()))
    
    sentiment_df = pd.DataFrame(sentiment_results)
    df = pd.concat([df.reset_index(drop=True), sentiment_df], axis=1)

    # 2. Convert timestamp and create engagement score
    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
    if platform == 'twitter':
        df['engagement_score'] = df.get('likes', 0) + df.get('retweets', 0) * 1.5 + df.get('replies', 0) * 1.2
    else: # reddit
        df['engagement_score'] = df.get('score', 0) + df.get('num_comments', 0) * 1.5
        
    # 3. Build Dashboard Layout
    st.header(f"Analysis Dashboard")
    
    # Create tabs for different views
    tab1, tab2, tab3, tab4 = st.tabs(["📊 Overview", "⏰ Activity Patterns", "🔬 Granular Breakdown", "💡 AI Insights"])

    with tab1:
        col1, col2 = st.columns(2)
        with col1:
            plot_sentiment_distribution(df)
        with col2:
            plot_engagement_timeline(df, platform)
        if platform == 'reddit':
            plot_subreddit_activity(df)

    with tab2:
        col1, col2 = st.columns(2)
        with col1:
            plot_activity_heatmap(df)
        with col2:
            st.markdown("#### Most Frequent Words")
            plot_word_cloud(df, platform)

    with tab3:
        st.markdown("### 🔬 Granular Activity Breakdown")
        link_col = 'tweet_link' if platform == 'twitter' else 'permalink'
        display_cols_map = {
            'twitter': ['sentiment', 'text', 'likes', 'retweets', 'replies', 'summary', link_col],
            'reddit': ['sentiment', 'content', 'score', 'num_comments', 'subreddit', 'summary', link_col]
        }
        display_cols = [col for col in display_cols_map.get(platform, []) if col in df.columns]
        st.dataframe(
            df[display_cols],
            column_config={link_col: st.column_config.LinkColumn(display_text="Go to Post")},
            use_container_width=True
        )
    
    # 4. Generate AI-powered text summaries and suggestions
    # Pass dataframe as JSON to make it hashable for caching
    overall_summary = generate_overall_summary(df.to_json())
    
    with tab4:
        st.markdown("### 🌟 Overall Summary")
        st.write(overall_summary)

        st.markdown("### 💡 Content Suggestions")
        suggestions = generate_content_suggestions(df.to_json(), platform)
        if suggestions and 'suggestions' in suggestions:
            for i, suggestion in enumerate(suggestions['suggestions']):
                with st.expander(f"Suggestion #{i+1}: {suggestion.get('title')}"):
                    st.markdown(f"**Content Idea:** {suggestion.get('content')}")
                    st.markdown(f"**Hashtags:** `{suggestion.get('hashtags')}`")
        else:
            st.warning("Could not generate content suggestions.")

        st.markdown("### 🔮 Engagement Prediction Tool")
        new_post_text = st.text_area("Enter text for a new post to predict its engagement:")
        if st.button("Predict Engagement"):
            if new_post_text:
                prediction = predict_engagement(new_post_text)
                st.write(prediction)
            else:
                st.warning("Please enter some text to predict.")

    return df, overall_summary
