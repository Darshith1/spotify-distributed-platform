import pandas as pd
import sys
import os

sys.path.append(os.getcwd())
from src.models.schema import SpotifyTrack
from src.utils.db import get_db

def diagnose():
    print("\n🕵️ DIAGNOSTIC REPORT")
    print("=" * 30)

    # 1. CHECK CSV SOURCE
    csv_path = "data/spotify_data.csv"
    try:
        df = pd.read_csv(csv_path, nrows=5)
        cols = [c.lower() for c in df.columns]
        if "valence" in cols:
            print("✅ CSV: 'valence' column FOUND.")
        else:
            print(f"❌ CSV: 'valence' column MISSING. Found: {cols}")
    except Exception as e:
        print(f"❌ CSV: Could not read file. {e}")

    # 2. CHECK PYDANTIC SCHEMA
    model_fields = SpotifyTrack.model_fields.keys()
    if "valence" in model_fields:
        print("✅ SCHEMA: 'valence' field FOUND in Python model.")
    else:
        print("❌ SCHEMA: 'valence' field MISSING in src/models/schema.py.")
        print("   -> Did you save the file after editing?")

    # 3. CHECK MONGODB DATA
    db = get_db()
    doc = db["spotify_clean"].find_one({}, {"_id": 0})
    if doc:
        if "valence" in doc:
            print("✅ DATABASE: 'valence' key FOUND in spotify_clean.")
        else:
            print("❌ DATABASE: 'valence' key MISSING in stored data.")
            print("   -> You likely need to run 'clean_silver.py' again.")
            print(f"   -> Sample Doc Keys: {list(doc.keys())}")
    else:
        print("❌ DATABASE: Collection is empty.")

    print("=" * 30 + "\n")

if __name__ == "__main__":
    diagnose()