import sys
import os
import pandas as pd
from datetime import datetime

# Add project root to path
sys.path.append(os.getcwd())
from src.utils.db import get_db

def run_aggregations():
    print("🚀 Starting Gold Layer (Aggregations)...")
    
    db = get_db()
    clean_coll = db["spotify_clean"]
    
    # ---------------------------------------------------------
    # AGGREGATION 1: Genre Statistics
    # (Avg Danceability, Energy, Popularity per Genre)
    # ---------------------------------------------------------
    print("📊 Computing Genre Statistics...")
    pipeline_genre = [
        {
            "$group": {
                "_id": "$genre",
                "avg_danceability": {"$avg": "$danceability"},
                "avg_energy": {"$avg": "$energy"},
                "avg_popularity": {"$avg": "$popularity"},
                "track_count": {"$sum": 1}
            }
        },
        { "$match": { "track_count": { "$gt": 50 } } }, # Filter out niche genres with < 50 tracks
        { "$sort": { "avg_popularity": -1 } },          # Sort by most popular
        { "$addFields": { "genre": "$_id" } },          # Rename _id to genre
        { "$project": { "_id": 0 } }                    # Hide _id
    ]
    
    results_genre = list(clean_coll.aggregate(pipeline_genre))
    
    if results_genre:
        db["analytics_genre_stats"].delete_many({})
        db["analytics_genre_stats"].insert_many(results_genre)
        print(f"✅ Saved {len(results_genre)} genre summaries to 'analytics_genre_stats'.")
    
    # ---------------------------------------------------------
    # AGGREGATION 2: Yearly Trends
    # (Evolution of music over decades)
    # ---------------------------------------------------------
    print("📅 Computing Yearly Trends...")
    pipeline_year = [
        {
            "$group": {
                "_id": "$year",
                "avg_duration_ms": {"$avg": "$duration_ms"},
                "avg_loudness": {"$avg": "$loudness"},
                "avg_tempo": {"$avg": "$tempo"},
                "total_tracks": {"$sum": 1}
            }
        },
        { "$sort": { "_id": 1 } }, # Sort by Year Ascending
        { "$addFields": { "year": "$_id" } },
        { "$project": { "_id": 0 } }
    ]
    
    results_year = list(clean_coll.aggregate(pipeline_year))
    
    if results_year:
        db["analytics_yearly_trends"].delete_many({})
        db["analytics_yearly_trends"].insert_many(results_year)
        print(f"✅ Saved {len(results_year)} yearly summaries to 'analytics_yearly_trends'.")

    # ---------------------------------------------------------
    # VERIFICATION
    # ---------------------------------------------------------
    print("\n🔍 Gold Layer Verification:")
    print(f"   Genre Stats: {db['analytics_genre_stats'].count_documents({})} docs")
    print(f"   Year Stats:  {db['analytics_yearly_trends'].count_documents({})} docs")

if __name__ == "__main__":
    run_aggregations()