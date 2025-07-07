"""
Data Exploration and Analysis for Fintech ETL Pipeline

This notebook explores the three datasets to understand their structure,
quality, and patterns before implementing the preprocessing stage.
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from config.spark_config import create_spark_session
from config.etl_config import RAW_DATA_PATHS, TRANSACTION_TYPES, USER_SEGMENTS
from src.utils import (
    setup_logging, 
    calculate_data_quality_metrics, 
    print_data_quality_report,
    show_sample_data,
    validate_dataframe_schema,
    filter_brazilian_users
)
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as spark_sum, when, isnan, isnull, lit, max as spark_max, min as spark_min, countDistinct
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType
from datetime import datetime

def load_datasets(spark: SparkSession) -> dict:
    """
    Load all three datasets from CSV files
    
    Args:
        spark (SparkSession): Spark session
    
    Returns:
        dict: Dictionary containing all loaded datasets
    """
    logger = setup_logging()
    datasets = {}
    
    try:
        # Load users dataset (dim_users.csv)
        logger.info("Loading users dataset...")
        users_df = spark.read.csv(RAW_DATA_PATHS["users"], header=True, inferSchema=True)
        datasets["users"] = users_df
        logger.info(f"Users dataset loaded: {users_df.count()} rows, {len(users_df.columns)} columns")
        
        # Load transactions dataset (bt_users_transactions.csv)
        logger.info("Loading transactions dataset...")
        transactions_df = spark.read.csv(RAW_DATA_PATHS["transactions"], header=True, inferSchema=True)
        datasets["transactions"] = transactions_df
        logger.info(f"Transactions dataset loaded: {transactions_df.count()} rows, {len(transactions_df.columns)} columns")
        
        # Load onboarding dataset (lk_onboarding.csv)
        logger.info("Loading onboarding dataset...")
        onboarding_df = spark.read.csv(RAW_DATA_PATHS["onboarding"], header=True, inferSchema=True)
        datasets["onboarding"] = onboarding_df
        logger.info(f"Onboarding dataset loaded: {onboarding_df.count()} rows, {len(onboarding_df.columns)} columns")
        
    except Exception as e:
        logger.error(f"Error loading datasets: {e}")
        raise
    
    return datasets

def analyze_dataset_schemas(datasets: dict):
    """
    Analyze and print schema information for all datasets
    
    Args:
        datasets (dict): Dictionary containing all datasets
    """
    print("\n" + "="*60)
    print("DATASET SCHEMA ANALYSIS")
    print("="*60)
    
    for name, df in datasets.items():
        print(f"\n--- {name.upper()} DATASET SCHEMA ---")
        df.printSchema()
        print(f"Columns: {df.columns}")

def analyze_user_ids_consistency(datasets: dict):
    """
    Analyze user ID consistency across datasets
    
    Args:
        datasets (dict): Dictionary containing all datasets
    """
    print("\n" + "="*60)
    print("USER ID CONSISTENCY ANALYSIS")
    print("="*60)
    
    # Get unique user IDs from each dataset
    users_ids = set(row.user_id for row in datasets["users"].select("user_id").collect())
    transactions_ids = set(row.user_id for row in datasets["transactions"].select("user_id").distinct().collect())
    onboarding_ids = set(row.user_id for row in datasets["onboarding"].select("user_id").collect())
    
    print(f"Unique user IDs in users dataset: {len(users_ids):,}")
    print(f"Unique user IDs in transactions dataset: {len(transactions_ids):,}")
    print(f"Unique user IDs in onboarding dataset: {len(onboarding_ids):,}")
    
    # Check overlaps
    users_transactions_overlap = users_ids.intersection(transactions_ids)
    users_onboarding_overlap = users_ids.intersection(onboarding_ids)
    transactions_onboarding_overlap = transactions_ids.intersection(onboarding_ids)
    all_three_overlap = users_ids.intersection(transactions_ids).intersection(onboarding_ids)
    
    print(f"\nOverlap between users and transactions: {len(users_transactions_overlap):,}")
    print(f"Overlap between users and onboarding: {len(users_onboarding_overlap):,}")
    print(f"Overlap between transactions and onboarding: {len(transactions_onboarding_overlap):,}")
    print(f"Users present in all three datasets: {len(all_three_overlap):,}")
    
    # Check for Brazilian users (MLB prefix)
    users_mlb = sum(1 for uid in users_ids if uid and uid.startswith("MLB"))
    transactions_mlb = sum(1 for uid in transactions_ids if uid and uid.startswith("MLB"))
    onboarding_mlb = sum(1 for uid in onboarding_ids if uid and uid.startswith("MLB"))
    
    print(f"\nBrazilian users (MLB prefix):")
    print(f"Users dataset: {users_mlb:,} ({users_mlb/len(users_ids)*100:.1f}%)")
    print(f"Transactions dataset: {transactions_mlb:,} ({transactions_mlb/len(transactions_ids)*100:.1f}%)")
    print(f"Onboarding dataset: {onboarding_mlb:,} ({onboarding_mlb/len(onboarding_ids)*100:.1f}%)")

def analyze_transaction_patterns(transactions_df):
    """
    Analyze transaction patterns and types
    
    Args:
        transactions_df: Transactions DataFrame
    """
    print("\n" + "="*60)
    print("TRANSACTION PATTERNS ANALYSIS")
    print("="*60)
    
    # Transaction type distribution
    print("\nTransaction type distribution:")
    transactions_df.groupBy("type").count().orderBy("type").show()
    
    # Segment distribution
    print("\nSegment distribution:")
    transactions_df.groupBy("segment").count().orderBy("segment").show()
    
    # Transaction type by segment
    print("\nTransaction type by segment:")
    transactions_df.groupBy("segment", "type").count().orderBy("segment", "type").show()
    
    # Date range analysis
    print("\nTransaction date range:")
    transactions_df.select(
        spark_min("transaction_dt").alias("min_date"),
        spark_max("transaction_dt").alias("max_date")
    ).show()

def analyze_onboarding_metrics(onboarding_df):
    """
    Analyze onboarding metrics and flags
    
    Args:
        onboarding_df: Onboarding DataFrame
    """
    print("\n" + "="*60)
    print("ONBOARDING METRICS ANALYSIS")
    print("="*60)
    
    total_users = onboarding_df.count()
    
    # Metric flags distribution
    metrics_summary = onboarding_df.agg(
        spark_sum("habito").alias("habito_count"),
        spark_sum("activacion").alias("activacion_count"),
        spark_sum("setup").alias("setup_count"),
        spark_sum("return").alias("return_count")
    ).collect()[0]
    
    print(f"Total users in onboarding: {total_users:,}")
    print(f"Users with habit: {metrics_summary['habito_count']:,} ({metrics_summary['habito_count']/total_users*100:.1f}%)")
    print(f"Users with activation: {metrics_summary['activacion_count']:,} ({metrics_summary['activacion_count']/total_users*100:.1f}%)")
    print(f"Users with setup: {metrics_summary['setup_count']:,} ({metrics_summary['setup_count']/total_users*100:.1f}%)")
    print(f"Users who returned: {metrics_summary['return_count']:,} ({metrics_summary['return_count']/total_users*100:.1f}%)")
    
    # First login date range
    print("\nFirst login date range:")
    onboarding_df.select(
        spark_min("first_login_dt").alias("min_first_login"),
        spark_max("first_login_dt").alias("max_first_login")
    ).show()

def analyze_user_segmentation(datasets: dict):
    """
    Analyze user segmentation patterns
    
    Args:
        datasets (dict): Dictionary containing all datasets
    """
    print("\n" + "="*60)
    print("USER SEGMENTATION ANALYSIS")
    print("="*60)
    
    # Get users who have transactions
    users_with_transactions = datasets["transactions"].select("user_id", "type").distinct()
    
    # Classify users based on transaction types
    payment_users = users_with_transactions.filter(
        col("type").isin(TRANSACTION_TYPES["PAYMENT"])
    ).select("user_id").distinct()
    
    collection_users = users_with_transactions.filter(
        col("type").isin(TRANSACTION_TYPES["COLLECTION"])
    ).select("user_id").distinct()
    
    # Users who only make payments (individuals)
    only_payment_users = payment_users.subtract(collection_users)
    
    # Users who make collections (sellers - might also make payments)
    seller_users = collection_users
    
    print(f"Users who make only payments (Individuals): {only_payment_users.count():,}")
    print(f"Users who make collections (Sellers): {seller_users.count():,}")
    print(f"Total users with transactions: {users_with_transactions.select('user_id').distinct().count():,}")
    
    # Users in onboarding but without transactions (Unknown segment)
    total_onboarding_users = datasets["onboarding"].select("user_id").distinct()
    users_with_any_transaction = datasets["transactions"].select("user_id").distinct()
    users_without_transactions = total_onboarding_users.subtract(users_with_any_transaction)
    
    print(f"Users in onboarding without transactions (Unknown): {users_without_transactions.count():,}")

def identify_data_quality_issues(datasets: dict):
    """
    Identify potential data quality issues
    
    Args:
        datasets (dict): Dictionary containing all datasets
    """
    print("\n" + "="*60)
    print("DATA QUALITY ISSUES IDENTIFICATION")
    print("="*60)
    
    for name, df in datasets.items():
        print(f"\n--- {name.upper()} DATASET ---")
        
        # Calculate and print data quality metrics
        quality_metrics = calculate_data_quality_metrics(df)
        print_data_quality_report(quality_metrics, name)
        
        # Check for duplicates
        total_rows = df.count()
        distinct_rows = df.distinct().count()
        if total_rows != distinct_rows:
            print(f"WARNING: {total_rows - distinct_rows} duplicate rows found!")
        
        # Check for specific issues based on dataset
        if name == "users":
            # Check for invalid user IDs
            invalid_user_ids = df.filter(~col("user_id").rlike("^MLB.*")).count()
            if invalid_user_ids > 0:
                print(f"WARNING: {invalid_user_ids} users without MLB prefix found!")
        
        elif name == "transactions":
            # Check for invalid transaction types
            valid_types = TRANSACTION_TYPES["PAYMENT"] + TRANSACTION_TYPES["COLLECTION"]
            invalid_types = df.filter(~col("type").isin(valid_types)).count()
            if invalid_types > 0:
                print(f"WARNING: {invalid_types} transactions with invalid types found!")
            
            # Check for invalid segments
            invalid_segments = df.filter(~col("segment").isin([1, 2])).count()
            if invalid_segments > 0:
                print(f"WARNING: {invalid_segments} transactions with invalid segments found!")

def main():
    """
    Main function to run the data exploration analysis
    """
    print("="*60)
    print("FINTECH ETL - DATA EXPLORATION AND ANALYSIS")
    print("="*60)
    
    # Create Spark session
    spark = create_spark_session("Data_Exploration")
    
    try:
        # Load datasets
        datasets = load_datasets(spark)
        
        # Show sample data
        for name, df in datasets.items():
            show_sample_data(df, name)
        
        # Analyze dataset schemas
        analyze_dataset_schemas(datasets)
        
        # Analyze user ID consistency
        analyze_user_ids_consistency(datasets)
        
        # Analyze transaction patterns
        analyze_transaction_patterns(datasets["transactions"])
        
        # Analyze onboarding metrics
        analyze_onboarding_metrics(datasets["onboarding"])
        
        # Analyze user segmentation
        analyze_user_segmentation(datasets)
        
        # Identify data quality issues
        identify_data_quality_issues(datasets)
        
        print("\n" + "="*60)
        print("DATA EXPLORATION COMPLETED SUCCESSFULLY!")
        print("="*60)
        
    except Exception as e:
        print(f"Error during data exploration: {e}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()