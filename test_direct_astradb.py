#!/usr/bin/env python3
"""
Direct AstraDB Connection Test using Cassandra Driver

This script tests direct connection to AstraDB and creates the required tables.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from dotenv import load_dotenv
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.policies import DCAwareRoundRobinPolicy
import ssl

# Load environment variables
load_dotenv()

def create_tables_cql():
    """
    Return CQL statements for creating tables
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

def test_direct_astradb_connection():
    """Test direct connection to AstraDB using Cassandra driver"""
    print("=" * 60)
    print("DIRECT ASTRADB CONNECTION TEST")
    print("=" * 60)
    
    try:
        # Get connection details
        bundle_path = os.getenv("ASTRA_DB_SECURE_BUNDLE_PATH", "astradb/secure-connect-big-data.zip")
        token = os.getenv("ASTRA_DB_TOKEN")
        keyspace = os.getenv("ASTRA_DB_KEYSPACE", "fintech_analytics")
        
        if not token:
            raise ValueError("ASTRA_DB_TOKEN environment variable is required")
        
        if not os.path.exists(bundle_path):
            raise FileNotFoundError(f"Secure connect bundle not found: {bundle_path}")
        
        print(f"🔄 Connecting to AstraDB keyspace '{keyspace}'...")
        print(f"📦 Using secure bundle: {bundle_path}")
        
        # Create authentication provider
        auth_provider = PlainTextAuthProvider('token', token)
        
        # Create cluster connection
        cluster = Cluster(
            cloud={
                'secure_connect_bundle': bundle_path
            },
            auth_provider=auth_provider,
            load_balancing_policy=DCAwareRoundRobinPolicy()
        )
        
        # Connect to the cluster
        session = cluster.connect()
        
        print("✅ Successfully connected to AstraDB!")
        
        # Set keyspace
        session.set_keyspace(keyspace)
        print(f"✅ Using keyspace: {keyspace}")
        
        # Test basic query
        print("🔄 Testing basic query...")
        rows = session.execute("SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name = %s", [keyspace])
        
        if rows.one():
            print(f"✅ Keyspace '{keyspace}' confirmed to exist")
        else:
            print(f"❌ Keyspace '{keyspace}' not found")
            return False
        
        # Create tables
        print("🔄 Creating tables...")
        cql_statements = create_tables_cql()
        
        created_tables = []
        for table_name, cql in cql_statements.items():
            try:
                session.execute(cql)
                created_tables.append(table_name)
                print(f"✅ Created table: {table_name}")
            except Exception as e:
                print(f"❌ Failed to create table {table_name}: {e}")
                return False
        
        # Verify tables exist
        print("🔄 Verifying tables...")
        for table_name in created_tables:
            rows = session.execute(
                "SELECT table_name FROM system_schema.tables WHERE keyspace_name = %s AND table_name = %s",
                [keyspace, table_name]
            )
            if rows.one():
                print(f"✅ Table verified: {table_name}")
            else:
                print(f"❌ Table not found: {table_name}")
        
        print("\n" + "=" * 60)
        print("🎉 ASTRADB SCHEMA SETUP SUCCESSFUL!")
        print("=" * 60)
        print(f"✅ Connected to AstraDB")
        print(f"✅ Keyspace: {keyspace}")
        print(f"✅ Tables created: {len(created_tables)}")
        print("\n📊 Ready for ETL data loading!")
        
        return True
        
    except Exception as e:
        print(f"❌ Direct AstraDB connection failed: {e}")
        return False
        
    finally:
        try:
            session.shutdown()
            cluster.shutdown()
        except:
            pass

def main():
    if test_direct_astradb_connection():
        print("\n🎯 Next step: Run the ETL pipeline with AstraDB tables ready!")
        return 0
    else:
        print("\n🔧 Please check your AstraDB configuration and try again.")
        return 1

if __name__ == "__main__":
    sys.exit(main())