import streamlit as st
import pandas as pd
import plotly.express as px
import sys
import os

sys.path.append(os.getcwd())
from src.utils.db import get_db

st.set_page_config(
    page_title="Spotify Big Data Platform",
    page_icon="🎵",
    layout="wide"
)

# REMOVED @st.cache_data temporarily to force a fresh DB read every time
def load_data():
    db = get_db()
    
    # 1. Fetch Gold Layer
    df_genre = pd.DataFrame(list(db["analytics_genre_stats"].find({}, {"_id": 0})))
    df_trends = pd.DataFrame(list(db["analytics_yearly_trends"].find({}, {"_id": 0})))
    
    # 2. Fetch Sample
    pipeline_sample = [{"$sample": {"size": 2000}}, {"$project": {"_id": 0}}]
    df_sample = pd.DataFrame(list(db["spotify_clean"].aggregate(pipeline_sample)))
    
    # 3. Counts
    total_tracks = db["spotify_clean"].estimated_document_count()
    
    return df_genre, df_trends, df_sample, total_tracks

try:
    df_genre, df_trends, df_sample, total_tracks = load_data()
except Exception as e:
    st.error(f"Database Connection Failed: {e}")
    st.stop()

# --- LAYOUT ---

st.title("🎵 Spotify Distributed Analytics Platform")
st.markdown(f"**Data Source:** MongoDB Replica Set (Docker) | **Volume:** {total_tracks:,} Tracks")
st.markdown("---")

col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Tracks Processed", f"{total_tracks/1000000:.2f}M")
col2.metric("Unique Genres", len(df_genre))
if not df_trends.empty:
    col3.metric("Data Range", f"{df_trends['year'].min()} - {df_trends['year'].max()}")
else:
    col3.metric("Data Range", "N/A")
    
if not df_genre.empty:
    col4.metric("Avg Popularity", f"{df_genre['avg_popularity'].mean():.1f}/100")

st.subheader("📈 Evolution of Music (1920 - 2023)")
tab1, tab2, tab3 = st.tabs(["Song Duration", "Loudness", "Tempo"])

# FIXED: Replaced use_container_width=True with explicit streamlit sizing
with tab1:
    if not df_trends.empty:
        fig_dur = px.line(df_trends, x="year", y="avg_duration_ms", title="Average Song Duration (ms)")
        st.plotly_chart(fig_dur, use_container_width=True)

with tab2:
    if not df_trends.empty:
        fig_loud = px.line(df_trends, x="year", y="avg_loudness", title="Average Loudness (dB)")
        st.plotly_chart(fig_loud, use_container_width=True)

with tab3:
    if not df_trends.empty:
        fig_tempo = px.line(df_trends, x="year", y="avg_tempo", title="Average Tempo (BPM)")
        st.plotly_chart(fig_tempo, use_container_width=True)

st.subheader("🏆 Top Genres by Popularity")
if not df_genre.empty:
    top_genres = df_genre.head(15)
    fig_bar = px.bar(
        top_genres, 
        x="avg_popularity", 
        y="genre", 
        orientation='h', 
        color="avg_danceability",
        title="Top 15 Genres"
    )
    fig_bar.update_layout(yaxis={'categoryorder':'total ascending'})
    st.plotly_chart(fig_bar, use_container_width=True)

st.subheader("🧪 The 'Mood Map': Energy vs. Valence")
st.markdown("Sample of 2,000 tracks.")

if not df_sample.empty and 'valence' in df_sample.columns:
    fig_scatter = px.scatter(
        df_sample, 
        x="valence", 
        y="energy", 
        color="genre", 
        hover_data=["artist_name", "track_name"],
        opacity=0.6,
        title="Energy vs. Valence Correlation"
    )
    st.plotly_chart(fig_scatter, use_container_width=True)
else:
    st.warning("⚠️ 'valence' column not found in data yet. Try clearing cache or re-running clean_silver.py")