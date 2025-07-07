#!/usr/bin/env python3
"""
Simple AstraDB connection test
"""

import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def test_env_variables():
    """Test if environment variables are loaded correctly"""
    print("🔍 Testing environment variables...")
    
    required_vars = [
        "ASTRA_DB_TOKEN",
        "ASTRA_DB_ID", 
        "ASTRA_DB_KEYSPACE",
        "ASTRA_DB_REGION"
    ]
    
    for var in required_vars:
        value = os.getenv(var)
        if value:
            # Mask sensitive information
            if "TOKEN" in var or "SECRET" in var:
                display_value = value[:10] + "..." + value[-5:] if len(value) > 15 else "***"
            else:
                display_value = value
            print(f"✅ {var}: {display_value}")
        else:
            print(f"❌ {var}: NOT SET")
            return False
    
    return True

def test_astrapy_import():
    """Test if astrapy can be imported"""
    try:
        from astrapy import DataAPIClient
        print("✅ astrapy module imported successfully")
        return True
    except ImportError as e:
        print(f"❌ Failed to import astrapy: {e}")
        return False

def test_astradb_connection():
    """Test basic AstraDB connection"""
    try:
        from astrapy import DataAPIClient
        
        token = os.getenv("ASTRA_DB_TOKEN")
        db_id = os.getenv("ASTRA_DB_ID")
        region = os.getenv("ASTRA_DB_REGION")
        
        api_endpoint = f"https://{db_id}-{region}.apps.astra.datastax.com"
        
        print(f"🔄 Connecting to: {api_endpoint}")
        
        # Initialize client
        client = DataAPIClient(token)
        
        # Get database
        database = client.get_database(api_endpoint)
        
        # Test connection by getting database info
        db_info = database.info()
        
        print(f"✅ Connected successfully!")
        print(f"   Database: {db_info.name}")
        print(f"   Environment: {db_info.environment}")
        
        # Try to get keyspace info (different approach for astrapy)
        try:
            # Test if we can work with the specified keyspace
            keyspace = os.getenv("ASTRA_DB_KEYSPACE", "fintech_analytics")
            print(f"   Target keyspace: {keyspace}")
        except Exception as ks_error:
            print(f"   Keyspace access: {ks_error}")
        
        return True
        
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return False

def main():
    print("=" * 60)
    print("ASTRADB CONNECTION TEST")
    print("=" * 60)
    
    # Test environment variables
    if not test_env_variables():
        print("\n❌ Environment variables test failed")
        return 1
    
    print()
    
    # Test astrapy import
    if not test_astrapy_import():
        print("\n❌ astrapy import test failed")
        return 1
        
    print()
    
    # Test AstraDB connection
    if not test_astradb_connection():
        print("\n❌ AstraDB connection test failed")
        return 1
    
    print("\n" + "=" * 60)
    print("🎉 ALL TESTS PASSED - ASTRADB READY!")
    print("=" * 60)
    
    return 0

if __name__ == "__main__":
    exit(main())