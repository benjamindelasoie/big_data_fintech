#!/usr/bin/env python3
"""
AstraDB Table Setup using AstraPy

This script creates the required tables in AstraDB for the ETL pipeline.
Uses astrapy which is already installed.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config.spark_config import get_astradb_connection_info
from astrapy import DataAPIClient
from dotenv import load_dotenv
import logging

# Load environment variables
load_dotenv()

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_astradb_tables():
    """
    Create tables in AstraDB for the fintech ETL pipeline
    """
    logger.info("🔄 Setting up AstraDB tables for Fintech ETL pipeline...")
    
    try:
        # Get connection info
        conn_info = get_astradb_connection_info()
        
        if not conn_info["token"]:
            raise ValueError("ASTRA_DB_TOKEN environment variable is required")
        
        # Initialize AstraDB client
        client = DataAPIClient(conn_info["token"])
        
        # Get database reference
        database = client.get_database(conn_info["api_endpoint"])
        
        logger.info(f"✅ Connected to AstraDB database: {conn_info['database_id']}")
        logger.info(f"📍 Region: {conn_info['region']}")
        logger.info(f"🗂️  Keyspace: {conn_info['keyspace']}")
        
        # Note: astrapy is primarily for document/collection operations
        # For table creation with CQL, we'll provide the CQL statements that can be executed manually
        # or through the AstraDB console
        
        logger.info("📝 AstraDB tables need to be created through CQL")
        logger.info("   The following CQL statements should be executed in AstraDB console:")
        
        cql_statements = get_table_creation_cql(conn_info['keyspace'])
        
        print("\n" + "=" * 80)
        print("CQL STATEMENTS FOR ASTRADB CONSOLE")
        print("=" * 80)
        
        for table_name, cql in cql_statements.items():
            print(f"\n-- {table_name.upper()} TABLE")
            print(cql)
        
        print("\n" + "=" * 80)
        print("INSTRUCTIONS:")
        print("=" * 80)
        print("1. Go to https://astra.datastax.com/")
        print("2. Open your 'big_data' database")
        print("3. Go to CQL Console")
        print("4. Execute the CQL statements above")
        print("5. Run the ETL pipeline again")
        print("=" * 80)
        
        # Test connection by getting database info
        logger.info("🔍 Testing connection...")
        
        # Try to get database info
        db_info = database.info()
        logger.info(f"✅ Database connection successful!")
        logger.info(f"   Database name: {db_info.name}")
        logger.info(f"   Environment: {db_info.environment}")
        
        logger.info("✅ AstraDB connection verified - tables need manual creation via CQL Console")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error setting up AstraDB tables: {e}")
        return False

def get_table_creation_cql(keyspace):
    """
    Return CQL statements for creating tables
    """
    
    cql_statements = {
        "user_segments": f"""
CREATE TABLE IF NOT EXISTS {keyspace}.user_segments (
    user_id TEXT PRIMARY KEY,
    user_segment TEXT,
    total_transactions INT,
    unique_transaction_types INT,
    transaction_days INT,
    first_transaction_dt TIMESTAMP,
    last_transaction_dt TIMESTAMP
);""",
        
        "user_metrics": f"""
CREATE TABLE IF NOT EXISTS {keyspace}.user_metrics (
    user_id TEXT PRIMARY KEY,
    first_login_dt TIMESTAMP,
    week_year TEXT,
    user_segment TEXT,
    total_transactions INT,
    transaction_days INT,
    drop_flag INT,
    return_flag INT,
    return_dt TIMESTAMP,
    activation_validated INT,
    activacion_dt TIMESTAMP,
    habit_validated INT,
    total_transactions_period INT,
    unique_transaction_days INT,
    setup_achieved INT,
    setup_dt TIMESTAMP,
    onboarding_end_dt TIMESTAMP,
    first_login_month INT,
    first_login_week INT
);""",
        
        "funnel_analysis": f"""
CREATE TABLE IF NOT EXISTS {keyspace}.funnel_analysis (
    dimension_type TEXT,
    dimension_value TEXT,
    total_users BIGINT,
    stage_1_registered BIGINT,
    stage_2_returned BIGINT,
    stage_3_activated BIGINT,
    stage_4_setup BIGINT,
    stage_5_habit BIGINT,
    PRIMARY KEY (dimension_type, dimension_value)
);""",
        
        "monthly_metrics": f"""
CREATE TABLE IF NOT EXISTS {keyspace}.monthly_metrics (
    year_month TEXT,
    user_segment TEXT,
    total_users BIGINT,
    returned_users BIGINT,
    activated_users BIGINT,
    setup_completed_users BIGINT,
    habit_achieved_users BIGINT,
    drop_rate DOUBLE,
    activation_rate DOUBLE,
    setup_rate DOUBLE,
    habit_rate DOUBLE,
    PRIMARY KEY (year_month, user_segment)
);""",
        
        "weekly_metrics": f"""
CREATE TABLE IF NOT EXISTS {keyspace}.weekly_metrics (
    year_week TEXT,
    user_segment TEXT,
    total_users BIGINT,
    returned_users BIGINT,
    activated_users BIGINT,
    setup_completed_users BIGINT,
    habit_achieved_users BIGINT,
    drop_rate DOUBLE,
    activation_rate DOUBLE,
    setup_rate DOUBLE,
    habit_rate DOUBLE,
    PRIMARY KEY (year_week, user_segment)
);"""
    }
    
    return cql_statements

def main():
    """
    Main function to set up AstraDB tables
    """
    print("=" * 70)
    print("ASTRADB TABLE SETUP - FINTECH ETL PIPELINE")
    print("=" * 70)
    
    # Setup tables
    success = create_astradb_tables()
    
    if success:
        print("\n" + "=" * 70)
        print("✅ ASTRADB CONNECTION VERIFIED!")
        print("=" * 70)
        print("\n📝 NEXT STEPS:")
        print("   1. Execute the CQL statements shown above in AstraDB console")
        print("   2. Run the ETL pipeline: python run_astradb_pipeline.py")
        print("   3. Verify data in AstraDB console")
        
    else:
        print("\n" + "=" * 70)
        print("❌ ASTRADB SETUP FAILED")
        print("=" * 70)
        print("\n🔧 Troubleshooting:")
        print("   1. Check your .env file has correct AstraDB credentials")
        print("   2. Verify your AstraDB token is valid")
        print("   3. Ensure the keyspace exists in your AstraDB instance")
        
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())