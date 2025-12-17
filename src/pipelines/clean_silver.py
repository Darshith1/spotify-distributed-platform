import sys
import os
import pandas as pd
from datetime import datetime
from pydantic import ValidationError
from typing import List

# Add project root to path
sys.path.append(os.getcwd())
from src.utils.db import get_db
from src.models.schema import SpotifyTrack

def batch_generator(cursor, batch_size=5000):
    """Yields lists of documents from the cursor in batches to save RAM."""
    batch = []
    for doc in cursor:
        batch.append(doc)
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        yield batch

def clean_data():
    print("🚀 Starting Silver Layer Processing (Streaming Mode)...")
    
    db = get_db()
    raw_coll = db["spotify_raw"]
    clean_coll = db["spotify_clean"]
    
    # Reset Clean Collection
    clean_coll.delete_many({})
    print("🧹 Cleared existing data in 'spotify_clean'.")

    # Get total count for progress bar
    total_docs = raw_coll.count_documents({})
    print(f"📥 Found {total_docs} raw records to process.")
    
    # Create a cursor (pointer), NOT a list. This uses almost 0 RAM.
    cursor = raw_coll.find({}, {"_id": 0})
    
    processed_count = 0
    valid_count = 0
    batch_size = 5000

    print("🛡️ Processing batches (Clean -> Validate -> Write)...")

    # We loop through the data 5,000 rows at a time
    for batch_data in batch_generator(cursor, batch_size):
        # 1. Convert Batch to Pandas (Small dataframe, safe for RAM)
        df = pd.DataFrame(batch_data)
        
        # 2. Vectorized Cleaning on the Batch
        # Drop duplicates within this batch
        df.drop_duplicates(subset=['track_id'], inplace=True)
        
        # Fill Nulls
        if 'popularity' in df.columns:
            df['popularity'] = df['popularity'].fillna(0)
        
        # 3. Pydantic Validation
        # This will now include 'valence' because you updated schema.py!
        valid_batch = []
        raw_records = df.to_dict("records")
        
        for record in raw_records:
            try:
                track = SpotifyTrack(**record)
                valid_batch.append(track.model_dump())
            except ValidationError:
                continue
        
        # 4. Write Valid Batch to DB
        if valid_batch:
            clean_coll.insert_many(valid_batch)
            valid_count += len(valid_batch)
        
        processed_count += len(batch_data)
        
        # Progress Bar
        percent = round((processed_count / total_docs) * 100, 1)
        print(f"   ⏳ Progress: {percent}% ({processed_count}/{total_docs}) | Valid: {valid_count}", end='\r')

    print(f"\n✅ SUCCESS: Pipeline Finished.")
    print(f"   Total Processed: {processed_count}")
    print(f"   Total Valid Saved: {valid_count}")
    print(f"   Rejected: {processed_count - valid_count}")

if __name__ == "__main__":
    clean_data()