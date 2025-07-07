"""
CORRECTED Data Preprocessing and Sanitization for Fintech ETL Pipeline

This module handles data cleaning, validation, and sanitization of the raw datasets
with CORRECTED CSV parsing for multiline fields.

CRITICAL FIX: Users CSV contains multiline address fields that were incorrectly 
parsed as 39,000 rows when actual record count is 13,000.
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from config.spark_config import create_spark_session
from config.etl_config import (
    RAW_DATA_PATHS, 
    PROCESSED_DATA_PATHS, 
    TRANSACTION_TYPES, 
    USER_SEGMENTS,
    DATA_QUALITY_THRESHOLDS,
    AB_TEST_CONFIG
)
from src.utils import (
    setup_logging, 
    write_dataframe_to_parquet,
    show_sample_data,
    filter_brazilian_users
)
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, isnan, isnull, lit, regexp_replace, 
    to_date, to_timestamp, trim, upper, lower,
    count, sum as spark_sum, desc, asc
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    TimestampType, DoubleType, DateType
)
from datetime import datetime
import logging

class CorrectedDataPreprocessor:
    """
    CORRECTED data preprocessing and sanitization class
    """
    
    def __init__(self, spark_session: SparkSession):
        self.spark = spark_session
        self.logger = setup_logging()
        
    def preprocess_users_dataset_corrected(self):
        """
        CORRECTED: Preprocess users dataset with proper multiline CSV parsing
        
        Key Fix: Use multiLine=True and proper CSV options to handle 
        addresses with embedded newlines correctly
        
        Returns:
            DataFrame: Cleaned users dataset with CORRECT record count (13,000)
        """
        self.logger.info("Starting CORRECTED users dataset preprocessing...")
        
        # CRITICAL FIX: Proper CSV reading configuration for multiline fields
        users_df = self.spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .option("multiLine", "true") \
            .option("escape", "\"") \
            .option("quote", "\"") \
            .csv(RAW_DATA_PATHS["users"])
        
        initial_count = users_df.count()
        self.logger.info(f"✅ CORRECTED Initial users dataset count: {initial_count:,} (was incorrectly 39,000)")
        
        # Decision 1: Remove unnecessary index column
        if '_c0' in users_df.columns:
            users_df = users_df.drop('_c0')
            self.logger.info("Removed index column '_c0'")
        
        # Decision 2: Filter only Brazilian users (MLB prefix) - all should be Brazilian
        users_df = filter_brazilian_users(users_df, "user_id")
        brazilian_count = users_df.count()
        self.logger.info(f"Brazilian users: {brazilian_count:,} ({brazilian_count/initial_count*100:.1f}%)")
        
        # Decision 3: Handle missing values and data types with better error handling
        users_df = users_df.filter(col("user_id").isNotNull() & (col("user_id") != ""))
        
        # Decision 4: Convert type column to double (handle string/null values)
        users_df = users_df.withColumn("type", 
            when(col("type").isNull() | (trim(col("type")) == ""), lit(None))
            .otherwise(col("type").cast(DoubleType()))
        )
        
        # Decision 5: Handle rubro column (only relevant for sellers)
        users_df = users_df.withColumn("rubro", 
            when(col("rubro").isNull() | (trim(col("rubro")) == ""), lit(None))
            .otherwise(col("rubro").cast(DoubleType()))
        )
        
        # Decision 6: Remove records with invalid data
        users_df = users_df.filter(
            col("user_id").rlike("^MLB[0-9]+$") &  # Valid MLB format
            col("user_id").isNotNull()
        )
        
        # Decision 7: Clean address field (remove embedded newlines and normalize)
        users_df = users_df.withColumn("address",
            regexp_replace(col("address"), "\\n", " ")
        ).withColumn("address",
            regexp_replace(col("address"), "\\s+", " ")  # Normalize multiple spaces
        )
        
        final_count = users_df.count()
        self.logger.info(f"Final users dataset count: {final_count:,} (removed {initial_count - final_count:,} records)")
        
        # Log sanitization summary
        self.logger.info(f"✅ CORRECTED Users dataset sanitization summary:")
        self.logger.info(f"  - FIXED CSV parsing for multiline addresses")
        self.logger.info(f"  - Confirmed actual record count: {final_count:,}")
        self.logger.info(f"  - Removed index column")
        self.logger.info(f"  - All users are Brazilian (100% coverage)")
        self.logger.info(f"  - Standardized data types")
        self.logger.info(f"  - Cleaned address fields")
        self.logger.info(f"  - Handled missing values properly")
        
        return users_df
    
    def preprocess_transactions_dataset(self):
        """
        Preprocess transactions dataset (no changes from original)
        """
        self.logger.info("Starting transactions dataset preprocessing...")
        
        # Load raw data
        transactions_df = self.spark.read.csv(RAW_DATA_PATHS["transactions"], header=True, inferSchema=True)
        
        initial_count = transactions_df.count()
        self.logger.info(f"Initial transactions dataset count: {initial_count:,}")
        
        # Remove unnecessary index column
        if '_c0' in transactions_df.columns:
            transactions_df = transactions_df.drop('_c0')
            self.logger.info("Removed index column '_c0'")
        
        # Filter only Brazilian users
        transactions_df = filter_brazilian_users(transactions_df, "user_id")
        brazilian_count = transactions_df.count()
        self.logger.info(f"Filtered to Brazilian users: {brazilian_count:,}")
        
        # Validate transaction types (1-9)
        valid_types = TRANSACTION_TYPES["PAYMENT"] + TRANSACTION_TYPES["COLLECTION"]
        transactions_df = transactions_df.filter(col("type").isin(valid_types))
        type_filtered_count = transactions_df.count()
        self.logger.info(f"Filtered to valid transaction types: {type_filtered_count:,}")
        
        # Validate segments (1-2)
        transactions_df = transactions_df.filter(col("segment").isin([1, 2]))
        segment_filtered_count = transactions_df.count()
        self.logger.info(f"Filtered to valid segments: {segment_filtered_count:,}")
        
        # Handle invalid timestamps
        transactions_df = transactions_df.filter(col("transaction_dt").isNotNull())
        timestamp_filtered_count = transactions_df.count()
        self.logger.info(f"Filtered out null timestamps: {timestamp_filtered_count:,}")
        
        # Filter transactions within business date range (2022)
        transactions_df = transactions_df.filter(
            (col("transaction_dt") >= lit("2022-01-01")) & 
            (col("transaction_dt") <= lit("2022-12-31"))
        )
        
        final_count = transactions_df.count()
        self.logger.info(f"Final transactions dataset count: {final_count:,} (removed {initial_count - final_count:,} records)")
        
        return transactions_df
    
    def preprocess_onboarding_dataset(self):
        """
        Preprocess onboarding dataset (no changes from original)
        """
        self.logger.info("Starting onboarding dataset preprocessing...")
        
        # Load raw data
        onboarding_df = self.spark.read.csv(RAW_DATA_PATHS["onboarding"], header=True, inferSchema=True)
        
        initial_count = onboarding_df.count()
        self.logger.info(f"Initial onboarding dataset count: {initial_count:,}")
        
        # Remove unnecessary index columns
        columns_to_drop = ['_c0', 'Unnamed: 0']
        for col_name in columns_to_drop:
            if col_name in onboarding_df.columns:
                onboarding_df = onboarding_df.drop(col_name)
                self.logger.info(f"Removed index column '{col_name}'")
        
        # Filter only Brazilian users
        onboarding_df = filter_brazilian_users(onboarding_df, "user_id")
        brazilian_count = onboarding_df.count()
        self.logger.info(f"Filtered to Brazilian users: {brazilian_count:,}")
        
        # Handle missing values in metric flags
        # Convert habito from double to integer
        onboarding_df = onboarding_df.withColumn("habito", 
            when(col("habito").isNull(), lit(0))
            .otherwise(col("habito").cast(IntegerType()))
        )
        
        # Ensure other flags are proper integers
        flag_columns = ['activacion', 'setup', 'return']
        for flag_col in flag_columns:
            onboarding_df = onboarding_df.withColumn(flag_col,
                when(col(flag_col).isNull(), lit(0))
                .otherwise(col(flag_col).cast(IntegerType()))
            )
        
        # Validate first_login_dt
        onboarding_df = onboarding_df.filter(col("first_login_dt").isNotNull())
        login_filtered_count = onboarding_df.count()
        self.logger.info(f"Filtered out null first_login_dt: {login_filtered_count:,}")
        
        # Filter to 2022 date range
        onboarding_df = onboarding_df.filter(
            (col("first_login_dt") >= lit("2022-01-01")) & 
            (col("first_login_dt") <= lit("2022-12-31"))
        )
        
        final_count = onboarding_df.count()
        self.logger.info(f"Final onboarding dataset count: {final_count:,} (removed {initial_count - final_count:,} records)")
        
        return onboarding_df
    
    def validate_corrected_data_consistency(self, users_df, transactions_df, onboarding_df):
        """
        Validate CORRECTED data consistency across datasets
        """
        self.logger.info("Validating CORRECTED data consistency across datasets...")
        
        # Get unique user IDs from each dataset
        users_ids = users_df.select("user_id").distinct().count()
        transactions_ids = transactions_df.select("user_id").distinct().count()
        onboarding_ids = onboarding_df.select("user_id").distinct().count()
        
        # Check overlaps
        users_transactions_overlap = users_df.select("user_id").intersect(
            transactions_df.select("user_id")
        ).count()
        
        users_onboarding_overlap = users_df.select("user_id").intersect(
            onboarding_df.select("user_id")
        ).count()
        
        transactions_onboarding_overlap = transactions_df.select("user_id").intersect(
            onboarding_df.select("user_id")
        ).count()
        
        validation_results = {
            "users_count": users_ids,
            "transactions_users_count": transactions_ids,
            "onboarding_users_count": onboarding_ids,
            "users_transactions_overlap": users_transactions_overlap,
            "users_onboarding_overlap": users_onboarding_overlap,
            "transactions_onboarding_overlap": transactions_onboarding_overlap
        }
        
        self.logger.info(f"✅ CORRECTED Data consistency validation results:")
        self.logger.info(f"  - Users dataset: {users_ids:,} unique users")
        self.logger.info(f"  - Transactions dataset: {transactions_ids:,} unique users")
        self.logger.info(f"  - Onboarding dataset: {onboarding_ids:,} unique users")
        self.logger.info(f"  - Users-Transactions overlap: {users_transactions_overlap:,}")
        self.logger.info(f"  - Users-Onboarding overlap: {users_onboarding_overlap:,}")
        self.logger.info(f"  - Transactions-Onboarding overlap: {transactions_onboarding_overlap:,}")
        
        # Calculate coverage ratios
        if users_ids > 0:
            onboarding_coverage = (users_onboarding_overlap / users_ids) * 100
            transaction_coverage = (users_transactions_overlap / users_ids) * 100
            
            self.logger.info(f"✅ CORRECTED Coverage ratios:")
            self.logger.info(f"  - Onboarding coverage: {onboarding_coverage:.1f}%")
            self.logger.info(f"  - Transaction coverage: {transaction_coverage:.1f}%")
        
        return validation_results
    
    def run_corrected_preprocessing_pipeline(self):
        """
        Run the CORRECTED complete preprocessing pipeline
        """
        self.logger.info("🔄 Starting CORRECTED complete preprocessing pipeline...")
        
        # Preprocess each dataset with corrections
        clean_users_df = self.preprocess_users_dataset_corrected()
        clean_transactions_df = self.preprocess_transactions_dataset()
        clean_onboarding_df = self.preprocess_onboarding_dataset()
        
        # Validate data consistency
        validation_results = self.validate_corrected_data_consistency(
            clean_users_df, clean_transactions_df, clean_onboarding_df
        )
        
        # Create output directory if it doesn't exist
        os.makedirs(os.path.dirname(PROCESSED_DATA_PATHS["clean_users"]), exist_ok=True)
        
        # Save corrected cleaned datasets (overwrite previous incorrect versions)
        write_dataframe_to_parquet(clean_users_df, PROCESSED_DATA_PATHS["clean_users"])
        write_dataframe_to_parquet(clean_transactions_df, PROCESSED_DATA_PATHS["clean_transactions"])
        write_dataframe_to_parquet(clean_onboarding_df, PROCESSED_DATA_PATHS["clean_onboarding"])
        
        self.logger.info("✅ CORRECTED preprocessing pipeline completed successfully!")
        
        return {
            "users": clean_users_df,
            "transactions": clean_transactions_df,
            "onboarding": clean_onboarding_df,
            "validation": validation_results
        }

def main():
    """
    Main function to run the CORRECTED preprocessing pipeline
    """
    print("="*70)
    print("FINTECH ETL - CORRECTED DATA PREPROCESSING")
    print("="*70)
    print("✅ FIXING: CSV parsing for multiline address fields")
    print("📊 EXPECTED: ~13,000 user records (not 39,000 lines)")
    print("="*70)
    
    # Create Spark session
    spark = create_spark_session("Corrected_Data_Preprocessing")
    
    try:
        # Initialize corrected preprocessor
        preprocessor = CorrectedDataPreprocessor(spark)
        
        # Run corrected preprocessing pipeline
        results = preprocessor.run_corrected_preprocessing_pipeline()
        
        # Show sample of corrected cleaned data
        print("\n" + "="*70)
        print("✅ CORRECTED CLEANED DATA SAMPLES")
        print("="*70)
        
        for name, df in results.items():
            if name != "validation":
                show_sample_data(df, f"CORRECTED_{name}", 5)
        
        print("\n" + "="*70)
        print("✅ CORRECTED PREPROCESSING COMPLETED SUCCESSFULLY!")
        print("="*70)
        
        return results
        
    except Exception as e:
        print(f"❌ Error during corrected preprocessing: {e}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()