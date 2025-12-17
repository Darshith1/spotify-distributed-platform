import os
from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database

# Load environment variables
load_dotenv()

def get_mongo_client():
    """Establishes a connection to the MongoDB Primary."""
    uri = os.getenv("MONGO_URI")
    if not uri:
        raise ValueError("MONGO_URI is not set in .env file")
    
    # directConnection=True forces the driver to talk to localhost:27017
    # without trying to resolve the internal docker names 'mongo1', 'mongo2'
    client = MongoClient(uri, directConnection=True)
    return client

def get_db(db_name: str = "spotify_db") -> Database:
    """Returns the database object."""
    client = get_mongo_client()
    return client[db_name]

def test_connection():
    """Simple test to verify we can talk to the cluster."""
    try:
        client = get_mongo_client()
        # The 'admin' command is a lightweight way to ping
        client.admin.command('ping')
        print("✅ SUCCESS: Connected to MongoDB Replica Set!")
        
        # Print topology to prove it's distributed
        topology = client.topology_description
        print(f"ℹ️  Topology Type: {topology.topology_type_name}")
    except Exception as e:
        print(f"❌ FAILED: {e}")

if __name__ == "__main__":
    test_connection()