"""
FINAL CORRECTED Data Preprocessing for Fintech ETL Pipeline

This addresses the multiline CSV parsing issue and provides the correct record counts.
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from config.spark_config import create_spark_session
from config.etl_config import RAW_DATA_PATHS
from src.utils import setup_logging, show_sample_data
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, regexp_replace, count, trim
from pyspark.sql.types import DoubleType, IntegerType

def load_corrected_users_dataset(spark):
    """
    Load users dataset with proper multiline CSV parsing
    """
    logger = setup_logging()
    logger.info("Loading users dataset with corrected CSV parsing...")
    
    # CRITICAL FIX: Proper CSV parsing for multiline addresses
    users_df = spark.read \
        .option("header", "true") \
        .option("multiLine", "true") \
        .option("escape", "\"") \
        .option("quote", "\"") \
        .csv(RAW_DATA_PATHS["users"])
    
    initial_count = users_df.count()
    logger.info(f"✅ CORRECTED: Users dataset has {initial_count:,} actual records (not 39,000 lines)")
    
    # Clean the data
    if '_c0' in users_df.columns:
        users_df = users_df.drop('_c0')
    
    # Filter Brazilian users
    users_df = users_df.filter(col("user_id").startswith("MLB"))
    brazilian_count = users_df.count()
    logger.info(f"Brazilian users: {brazilian_count:,}")
    
    # Handle data type issues - use try_cast for better error handling
    users_df = users_df.withColumn("type_clean",
        when(col("type").isNull() | (trim(col("type")) == ""), lit(None))
        .otherwise(col("type").cast(DoubleType()))
    ).withColumn("rubro_clean",
        when(col("rubro").isNull() | (trim(col("rubro")) == ""), lit(None))
        .otherwise(col("rubro").cast(DoubleType()))
    ).drop("type", "rubro") \
     .withColumnRenamed("type_clean", "type") \
     .withColumnRenamed("rubro_clean", "rubro")
    
    # Clean address field
    users_df = users_df.withColumn("address",
        regexp_replace(col("address"), "\\n", " ")
    )
    
    final_count = users_df.count()
    logger.info(f"Final clean users: {final_count:,}")
    
    return users_df

def load_corrected_datasets(spark):
    """
    Load all datasets with corrections
    """
    logger = setup_logging()
    
    # Load corrected users dataset
    users_df = load_corrected_users_dataset(spark)
    
    # Load other datasets (no changes needed)
    transactions_df = spark.read.csv(RAW_DATA_PATHS["transactions"], header=True, inferSchema=True)
    if '_c0' in transactions_df.columns:
        transactions_df = transactions_df.drop('_c0')
    transactions_df = transactions_df.filter(col("user_id").startswith("MLB"))
    
    onboarding_df = spark.read.csv(RAW_DATA_PATHS["onboarding"], header=True, inferSchema=True)
    if '_c0' in onboarding_df.columns:
        onboarding_df = onboarding_df.drop('_c0')
    if 'Unnamed: 0' in onboarding_df.columns:
        onboarding_df = onboarding_df.drop('Unnamed: 0')
    onboarding_df = onboarding_df.filter(col("user_id").startswith("MLB"))
    
    logger.info(f"Dataset counts:")
    logger.info(f"  - Users: {users_df.count():,} (CORRECTED)")
    logger.info(f"  - Transactions: {transactions_df.count():,}")
    logger.info(f"  - Onboarding: {onboarding_df.count():,}")
    
    return {
        "users": users_df,
        "transactions": transactions_df,
        "onboarding": onboarding_df
    }

def analyze_corrected_data_impact(datasets):
    """
    Analyze the impact of the correction
    """
    logger = setup_logging()
    
    users_count = datasets["users"].count()
    transactions_users = datasets["transactions"].select("user_id").distinct().count()
    onboarding_users = datasets["onboarding"].select("user_id").distinct().count()
    
    # Check overlaps
    users_onboarding_overlap = datasets["users"].select("user_id").intersect(
        datasets["onboarding"].select("user_id")
    ).count()
    
    users_transactions_overlap = datasets["users"].select("user_id").intersect(
        datasets["transactions"].select("user_id")
    ).count()
    
    logger.info(f"CORRECTED Data Analysis:")
    logger.info(f"  - Users dataset: {users_count:,} records")
    logger.info(f"  - Users in onboarding: {onboarding_users:,}")
    logger.info(f"  - Users with transactions: {transactions_users:,}")
    logger.info(f"  - Users-Onboarding overlap: {users_onboarding_overlap:,}")
    logger.info(f"  - Users-Transactions overlap: {users_transactions_overlap:,}")
    
    # Calculate key ratios
    if users_count > 0:
        coverage_ratio = (users_onboarding_overlap / users_count) * 100
        transaction_ratio = (users_transactions_overlap / users_count) * 100
        
        logger.info(f"CORRECTED Ratios:")
        logger.info(f"  - Users coverage in onboarding: {coverage_ratio:.1f}%")
        logger.info(f"  - Users with transactions: {transaction_ratio:.1f}%")
    
    return {
        "users_count": users_count,
        "onboarding_users": onboarding_users,
        "transaction_users": transactions_users,
        "coverage_ratio": coverage_ratio if users_count > 0 else 0,
        "transaction_ratio": transaction_ratio if users_count > 0 else 0
    }

def main():
    """
    Main function to demonstrate the correction
    """
    print("="*70)
    print("FINTECH ETL - DATA PARSING CORRECTION ANALYSIS")
    print("="*70)
    print("\n🔍 ISSUE: CSV files with multiline address fields were incorrectly parsed")
    print("   Original count: 39,000 'rows' (actually lines, not records)")
    print("   Correction: Proper CSV parsing with multiLine=True")
    print("\n" + "="*70)
    
    spark = create_spark_session("Data_Parsing_Correction")
    
    try:
        # Load corrected datasets
        datasets = load_corrected_datasets(spark)
        
        # Show sample data
        print("\n📊 CORRECTED DATA SAMPLES:")
        for name, df in datasets.items():
            show_sample_data(df, f"CORRECTED_{name.upper()}", 5)
        
        # Analyze the impact
        print("\n📈 IMPACT ANALYSIS:")
        impact = analyze_corrected_data_impact(datasets)
        
        print("\n" + "="*70)
        print("✅ CORRECTION SUMMARY:")
        print(f"   - BEFORE: 39,000 lines (incorrect parsing)")
        print(f"   - AFTER: {impact['users_count']:,} actual user records")
        print(f"   - Data reduction: {((39000 - impact['users_count']) / 39000) * 100:.1f}%")
        print(f"   - This significantly impacts all downstream metrics!")
        print("="*70)
        
    except Exception as e:
        print(f"❌ Error during analysis: {e}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()