#!/usr/bin/env python3
"""
Test Spark AstraDB Connection

This script tests the Spark Cassandra connector with AstraDB configuration.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config.spark_config import create_spark_session, get_cassandra_config
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def test_spark_astradb_connection():
    """Test Spark connection to AstraDB"""
    print("=" * 60)
    print("SPARK ASTRADB CONNECTION TEST")
    print("=" * 60)
    
    try:
        # Create Spark session with AstraDB configuration
        print("🔄 Creating Spark session with AstraDB configuration...")
        spark = create_spark_session("AstraDB_Test", use_astradb=True)
        
        print("✅ Spark session created successfully")
        
        # Get Cassandra configuration
        cassandra_config = get_cassandra_config(use_astradb=True)
        keyspace = cassandra_config["keyspace"]
        
        print(f"🗂️  Target keyspace: {keyspace}")
        
        # Try to create a simple test table
        print("🔄 Testing table creation...")
        
        # Create a simple DataFrame
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType
        from pyspark.sql import Row
        
        schema = StructType([
            StructField("id", StringType(), True),
            StructField("test_value", IntegerType(), True)
        ])
        
        test_data = [Row(id="test1", test_value=123)]
        test_df = spark.createDataFrame(test_data, schema)
        
        print(f"✅ Test DataFrame created with {test_df.count()} rows")
        
        # Try to write to AstraDB
        print(f"🔄 Attempting to write test data to AstraDB keyspace '{keyspace}'...")
        
        test_df.write \
            .format("org.apache.spark.sql.cassandra") \
            .options(table="spark_test", keyspace=keyspace) \
            .mode("overwrite") \
            .save()
        
        print("✅ Successfully wrote test data to AstraDB!")
        
        # Try to read it back
        print("🔄 Attempting to read test data back from AstraDB...")
        
        read_df = spark.read \
            .format("org.apache.spark.sql.cassandra") \
            .options(table="spark_test", keyspace=keyspace) \
            .load()
        
        print(f"✅ Successfully read {read_df.count()} rows from AstraDB!")
        read_df.show()
        
        # Clean up test table
        print("🧹 Cleaning up test table...")
        spark.sql(f"DROP TABLE IF EXISTS {keyspace}.spark_test")
        
        print("\n" + "=" * 60)
        print("🎉 SPARK ASTRADB CONNECTION TEST PASSED!")
        print("=" * 60)
        
        return True
        
    except Exception as e:
        print(f"❌ Spark AstraDB connection test failed: {e}")
        print("\n🔧 Troubleshooting suggestions:")
        print("   1. Check that secure connect bundle exists")
        print("   2. Verify AstraDB token is valid")
        print("   3. Ensure keyspace exists in AstraDB")
        print("   4. Check network connectivity")
        return False
        
    finally:
        try:
            spark.stop()
        except:
            pass

def verify_astradb_config():
    """Verify AstraDB configuration files and environment variables"""
    print("🔍 Verifying AstraDB configuration...")
    
    # Check environment variables
    required_vars = ["ASTRA_DB_TOKEN", "ASTRA_DB_KEYSPACE", "ASTRA_DB_SECURE_BUNDLE_PATH"]
    
    for var in required_vars:
        value = os.getenv(var)
        if value:
            if "TOKEN" in var:
                display_value = value[:10] + "..." + value[-5:]
            else:
                display_value = value
            print(f"✅ {var}: {display_value}")
        else:
            print(f"❌ {var}: NOT SET")
            return False
    
    # Check secure connect bundle
    bundle_path = os.getenv("ASTRA_DB_SECURE_BUNDLE_PATH")
    if bundle_path and os.path.exists(bundle_path):
        print(f"✅ Secure connect bundle exists: {bundle_path}")
    else:
        print(f"❌ Secure connect bundle not found: {bundle_path}")
        return False
    
    return True

def main():
    if not verify_astradb_config():
        print("\n❌ Configuration verification failed")
        return 1
    
    print()
    
    if test_spark_astradb_connection():
        return 0
    else:
        return 1

if __name__ == "__main__":
    sys.exit(main())