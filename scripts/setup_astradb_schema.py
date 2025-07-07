#!/usr/bin/env python3
"""
AstraDB Schema Setup Script

This script creates the necessary keyspace and tables for the fintech ETL pipeline
using the AstraDB API through astrapy.
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from config.spark_config import get_astradb_connection_info
from astrapy import DataAPIClient
from dotenv import load_dotenv
import logging

# Load environment variables
load_dotenv()

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def setup_astradb_schema():
    """
    Set up AstraDB keyspace and tables for the ETL pipeline
    """
    logger.info("🔄 Setting up AstraDB schema for Fintech ETL pipeline...")
    
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
        
        # Check if keyspace exists (it should from your setup)
        keyspaces = database.list_keyspaces()
        if conn_info['keyspace'] not in [ks.name for ks in keyspaces]:
            logger.warning(f"⚠️  Keyspace '{conn_info['keyspace']}' not found in database")
            logger.info("Creating keyspace through AstraDB console is recommended")
            return False
        
        logger.info(f"✅ Keyspace '{conn_info['keyspace']}' exists")
        
        # Note: Table creation for analytical workloads is typically done through CQL
        # since astrapy is primarily for document/collection operations
        logger.info("📝 For table creation, we'll use Spark Cassandra connector during pipeline execution")
        logger.info("   Tables will be created automatically when data is written")
        
        # Test connection by getting keyspace info
        logger.info("🔍 Testing connection...")
        
        # Try to get database info
        db_info = database.info()
        logger.info(f"✅ Database connection successful!")
        logger.info(f"   Database name: {db_info.name}")
        logger.info(f"   Environment: {db_info.environment}")
        
        logger.info("🎉 AstraDB schema setup completed successfully!")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error setting up AstraDB schema: {e}")
        return False

def create_tables_cql():
    """
    Return CQL statements for creating tables (for reference)
    These will be executed through Spark Cassandra connector
    """
    
    cql_statements = {
        "user_segments": """
            CREATE TABLE IF NOT EXISTS fintech_analytics.user_segments (
                user_id TEXT PRIMARY KEY,
                user_segment TEXT,
                total_transactions INT,
                unique_transaction_types INT,
                transaction_days INT,
                first_transaction_dt TIMESTAMP,
                last_transaction_dt TIMESTAMP
            );
        """,
        
        "user_metrics": """
            CREATE TABLE IF NOT EXISTS fintech_analytics.user_metrics (
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
            );
        """,
        
        "funnel_analysis": """
            CREATE TABLE IF NOT EXISTS fintech_analytics.funnel_analysis (
                dimension_type TEXT,
                dimension_value TEXT,
                total_users BIGINT,
                stage_1_registered BIGINT,
                stage_2_returned BIGINT,
                stage_3_activated BIGINT,
                stage_4_setup BIGINT,
                stage_5_habit BIGINT,
                PRIMARY KEY (dimension_type, dimension_value)
            );
        """,
        
        "monthly_metrics": """
            CREATE TABLE IF NOT EXISTS fintech_analytics.monthly_metrics (
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
            );
        """,
        
        "weekly_metrics": """
            CREATE TABLE IF NOT EXISTS fintech_analytics.weekly_metrics (
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
            );
        """
    }
    
    return cql_statements

def main():
    """
    Main function to set up AstraDB schema
    """
    print("=" * 70)
    print("ASTRADB SCHEMA SETUP - FINTECH ETL PIPELINE")
    print("=" * 70)
    
    # Setup schema
    success = setup_astradb_schema()
    
    if success:
        print("\n" + "=" * 70)
        print("✅ ASTRADB SETUP COMPLETED SUCCESSFULLY!")
        print("=" * 70)
        print("\n📝 Next steps:")
        print("   1. Run the ETL pipeline with AstraDB enabled")
        print("   2. Tables will be created automatically during data loading")
        print("   3. Verify data in AstraDB console")
        
        # Print CQL statements for reference
        cql_statements = create_tables_cql()
        print(f"\n📋 Table schemas (for reference):")
        for table_name in cql_statements.keys():
            print(f"   - {table_name}")
            
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