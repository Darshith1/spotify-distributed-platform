import pandas as pd
from datetime import datetime
import sys
import os

# Add project root to python path
sys.path.append(os.getcwd())
from src.utils.db import get_db

def ingest_raw_data(file_path: str):
    print(f"🚀 Starting Ingestion from {file_path}...")
    
    # 1. Read CSV
    # We use low_memory=False to prevent warning on mixed types
    try:
        df = pd.read_csv(file_path, low_memory=False)
        print(f"✅ CSV Loaded. Rows: {len(df)}")
    except FileNotFoundError:
        print("❌ Error: CSV file not found. Check 'data/spotify_data.csv'")
        return

    # 2. Volume Check (Rubric Verification)
    if len(df) < 750000:
        print(f"⚠️ WARNING: spotify_data has {len(df)} rows. Rubric requires 750,000+.")
    else:
        print(f"💪 Excellent. spotify_data meets Big Data volume requirements.")

    # 3. Add Metadata
    df['ingestion_timestamp'] = datetime.utcnow()

    # 4. Connect to DB
    db = get_db()
    collection = db["spotify_raw"]
    
    # Clear old data to ensure clean state
    collection.delete_many({})
    print("🧹 Cleared existing data in 'spotify_raw'.")

    # 5. Batch Insertion (Vital for Large spotify_datas)
    # MongoDB has a 16MB document limit per batch. 
    # We send 5,000 rows at a time to be safe and fast.
    batch_size = 5000
    total_records = len(df)
    records = df.to_dict("records")
    
    print(f"🚚 Inserting {total_records} records into MongoDB...")
    
    for i in range(0, total_records, batch_size):
        batch = records[i : i + batch_size]
        if batch:
            collection.insert_many(batch)
            # Progress bar effect
            percent = round((i / total_records) * 100, 1)
            print(f"   ⏳ Progress: {percent}% ({i}/{total_records})", end='\r')
        
    print(f"\n✅ SUCCESS: Ingested {total_records} documents into 'spotify_raw'.")
    
    # 6. Final Verification
    count = collection.count_documents({})
    print(f"🔍 Database Verification: Collection contains {count} documents.")

if __name__ == "__main__":
    CSV_PATH = "data/spotify_data.csv" 
    ingest_raw_data(CSV_PATH)