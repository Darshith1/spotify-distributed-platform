import pytest
import sys
import os
from pydantic import ValidationError

# Add project root to path so we can import src
sys.path.append(os.getcwd())
from src.utils.db import get_db
from src.models.schema import SpotifyTrack

# ---------------------------------------------------------
# TEST 1: Infrastructure Check
# Does the database exist and is it reachable?
# ---------------------------------------------------------
def test_database_connection():
    try:
        db = get_db()
        # The 'ping' command is the standard heartbeat check
        db.command('ping')
        assert True
    except Exception as e:
        pytest.fail(f"Database connection failed: {e}")

# ---------------------------------------------------------
# TEST 2: Volume Verification (The "Big Data" Test)
# Did we actually ingest enough data?
# ---------------------------------------------------------
def test_raw_data_volume():
    db = get_db()
    count = db["spotify_raw"].count_documents({})
    # Rubric requires > 750,000
    assert count > 750000, f"Expected > 750k rows, found {count}"

# ---------------------------------------------------------
# TEST 3: Data Integrity (Schema Logic)
# Does our Pydantic model correctly catch bad data?
# ---------------------------------------------------------
def test_schema_validation():
    # Case A: Valid Data (UPDATED to include valence)
    valid_data = {
        "track_id": "123", "artist_name": "Test Artist", "track_name": "Song",
        "year": 2020, "genre": "pop", "danceability": 0.5, "energy": 0.8,
        "loudness": -5.0, "tempo": 120.0, "duration_ms": 200000,
        "valence": 0.5  # <--- Added this required field
    }
    try:
        SpotifyTrack(**valid_data)
    except ValidationError as e:
        pytest.fail(f"Pydantic rejected valid data! Error: {e}")

    # Case B: Invalid Data (Negative Tempo)
    invalid_data = valid_data.copy()
    invalid_data["tempo"] = -10.0 
    
    with pytest.raises(ValidationError):
        SpotifyTrack(**invalid_data)

# ---------------------------------------------------------
# TEST 4: Gold Layer Existence
# Did the aggregations actually run?
# ---------------------------------------------------------
def test_gold_layer_content():
    db = get_db()
    genre_stats = db["analytics_genre_stats"].count_documents({})
    year_stats = db["analytics_yearly_trends"].count_documents({})
    
    assert genre_stats > 0, "Genre analytics collection is empty!"
    assert year_stats > 0, "Yearly trends collection is empty!"