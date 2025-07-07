"""
Data Preprocessing and Sanitization for Fintech ETL Pipeline

This module handles data cleaning, validation, and sanitization of the raw datasets
before they are processed in the transformation stage.

Data Sanitization Decisions:
1. Remove unnecessary index columns (_c0, Unnamed: 0)
2. Handle missing values according to business logic
3. Standardize data types and formats
4. Filter out invalid or corrupted records
5. Ensure data consistency across datasets
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

class DataPreprocessor:
    """
    Data preprocessing and sanitization class
    """
    
    def __init__(self, spark_session: SparkSession):
        self.spark = spark_session
        self.logger = setup_logging()
        
    def preprocess_users_dataset(self):
        """
        Preprocess and sanitize users dataset (dim_users.csv)
        
        Data Sanitization Decisions:
        1. Remove index column (_c0)
        2. Filter only Brazilian users (MLB prefix)
        3. Handle missing values in rubro (only for sellers)
        4. Standardize user_id format
        5. Convert type to integer
        6. Remove records with invalid user_id format
        
        Returns:
            DataFrame: Cleaned users dataset
        """
        self.logger.info("Starting users dataset preprocessing...")
        
        # Load raw data
        users_df = self.spark.read.csv(RAW_DATA_PATHS["users"], header=True, inferSchema=True)
        
        initial_count = users_df.count()
        self.logger.info(f"Initial users dataset count: {initial_count:,}")
        
        # Decision 1: Remove unnecessary index column
        if '_c0' in users_df.columns:
            users_df = users_df.drop('_c0')
            self.logger.info("Removed index column '_c0'")
        
        # Decision 2: Filter only Brazilian users (MLB prefix)
        users_df = filter_brazilian_users(users_df, "user_id")
        brazilian_count = users_df.count()
        self.logger.info(f"Filtered to Brazilian users: {brazilian_count:,} ({brazilian_count/initial_count*100:.1f}%)")
        
        # Decision 3: Handle missing values and data types
        users_df = users_df.filter(col("user_id").isNotNull() & (col("user_id") != ""))
        
        # Decision 4: Convert type column to integer (handle string values)
        users_df = users_df.withColumn("type", 
            when(col("type").rlike("^[0-9]+$"), col("type").cast(IntegerType()))
            .otherwise(lit(None))
        )
        
        # Decision 5: Handle rubro column (only relevant for sellers)
        users_df = users_df.withColumn("rubro", 
            when(col("rubro").isNull() | (col("rubro") == ""), lit(None))
            .otherwise(col("rubro").cast(DoubleType()))
        )
        
        # Decision 6: Remove records with invalid data
        users_df = users_df.filter(
            col("user_id").rlike("^MLB[0-9]+$") &  # Valid MLB format
            col("user_id").isNotNull()
        )
        
        final_count = users_df.count()
        self.logger.info(f"Final users dataset count: {final_count:,} (removed {initial_count - final_count:,} records)")
        
        # Log sanitization summary
        self.logger.info(f"Users dataset sanitization summary:")
        self.logger.info(f"  - Removed index column")
        self.logger.info(f"  - Filtered to Brazilian users only")
        self.logger.info(f"  - Standardized data types")
        self.logger.info(f"  - Removed invalid user_id formats")
        self.logger.info(f"  - Handled missing values in rubro column")
        
        return users_df
    
    def preprocess_transactions_dataset(self):
        """
        Preprocess and sanitize transactions dataset (bt_users_transactions.csv)
        
        Data Sanitization Decisions:
        1. Remove index column (_c0)
        2. Filter only Brazilian users (MLB prefix)
        3. Validate transaction types (1-9)
        4. Validate segments (1-2)
        5. Handle invalid timestamps
        6. Remove transactions outside business date range
        
        Returns:
            DataFrame: Cleaned transactions dataset
        """
        self.logger.info("Starting transactions dataset preprocessing...")
        
        # Load raw data
        transactions_df = self.spark.read.csv(RAW_DATA_PATHS["transactions"], header=True, inferSchema=True)
        
        initial_count = transactions_df.count()
        self.logger.info(f"Initial transactions dataset count: {initial_count:,}")
        
        # Decision 1: Remove unnecessary index column
        if '_c0' in transactions_df.columns:
            transactions_df = transactions_df.drop('_c0')
            self.logger.info("Removed index column '_c0'")
        
        # Decision 2: Filter only Brazilian users
        transactions_df = filter_brazilian_users(transactions_df, "user_id")
        brazilian_count = transactions_df.count()
        self.logger.info(f"Filtered to Brazilian users: {brazilian_count:,}")
        
        # Decision 3: Validate transaction types (1-9)
        valid_types = TRANSACTION_TYPES["PAYMENT"] + TRANSACTION_TYPES["COLLECTION"]
        transactions_df = transactions_df.filter(col("type").isin(valid_types))
        type_filtered_count = transactions_df.count()
        self.logger.info(f"Filtered to valid transaction types: {type_filtered_count:,}")
        
        # Decision 4: Validate segments (1-2)
        transactions_df = transactions_df.filter(col("segment").isin([1, 2]))
        segment_filtered_count = transactions_df.count()
        self.logger.info(f"Filtered to valid segments: {segment_filtered_count:,}")
        
        # Decision 5: Handle invalid timestamps
        transactions_df = transactions_df.filter(col("transaction_dt").isNotNull())
        timestamp_filtered_count = transactions_df.count()
        self.logger.info(f"Filtered out null timestamps: {timestamp_filtered_count:,}")
        
        # Decision 6: Filter transactions within business date range (2022)
        transactions_df = transactions_df.filter(
            (col("transaction_dt") >= lit("2022-01-01")) & 
            (col("transaction_dt") <= lit("2022-12-31"))
        )
        
        final_count = transactions_df.count()
        self.logger.info(f"Final transactions dataset count: {final_count:,} (removed {initial_count - final_count:,} records)")
        
        # Log sanitization summary
        self.logger.info(f"Transactions dataset sanitization summary:")
        self.logger.info(f"  - Removed index column")
        self.logger.info(f"  - Filtered to Brazilian users only")
        self.logger.info(f"  - Validated transaction types (1-9)")
        self.logger.info(f"  - Validated segments (1-2)")
        self.logger.info(f"  - Removed invalid timestamps")
        self.logger.info(f"  - Filtered to 2022 date range")
        
        return transactions_df
    
    def preprocess_onboarding_dataset(self):
        """
        Preprocess and sanitize onboarding dataset (lk_onboarding.csv)
        
        Data Sanitization Decisions:
        1. Remove index columns (_c0, Unnamed: 0)
        2. Filter only Brazilian users (MLB prefix)
        3. Handle missing values in metric flags
        4. Standardize habito column (convert to integer)
        5. Validate first_login_dt
        6. Handle missing date columns
        
        Returns:
            DataFrame: Cleaned onboarding dataset
        """
        self.logger.info("Starting onboarding dataset preprocessing...")
        
        # Load raw data
        onboarding_df = self.spark.read.csv(RAW_DATA_PATHS["onboarding"], header=True, inferSchema=True)
        
        initial_count = onboarding_df.count()
        self.logger.info(f"Initial onboarding dataset count: {initial_count:,}")
        
        # Decision 1: Remove unnecessary index columns
        columns_to_drop = ['_c0', 'Unnamed: 0']
        for col_name in columns_to_drop:
            if col_name in onboarding_df.columns:
                onboarding_df = onboarding_df.drop(col_name)
                self.logger.info(f"Removed index column '{col_name}'")
        
        # Decision 2: Filter only Brazilian users
        onboarding_df = filter_brazilian_users(onboarding_df, "user_id")
        brazilian_count = onboarding_df.count()
        self.logger.info(f"Filtered to Brazilian users: {brazilian_count:,}")
        
        # Decision 3: Handle missing values in metric flags
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
        
        # Decision 4: Validate first_login_dt
        onboarding_df = onboarding_df.filter(col("first_login_dt").isNotNull())
        login_filtered_count = onboarding_df.count()
        self.logger.info(f"Filtered out null first_login_dt: {login_filtered_count:,}")
        
        # Decision 5: Filter to 2022 date range
        onboarding_df = onboarding_df.filter(
            (col("first_login_dt") >= lit("2022-01-01")) & 
            (col("first_login_dt") <= lit("2022-12-31"))
        )
        
        final_count = onboarding_df.count()
        self.logger.info(f"Final onboarding dataset count: {final_count:,} (removed {initial_count - final_count:,} records)")
        
        # Log sanitization summary
        self.logger.info(f"Onboarding dataset sanitization summary:")
        self.logger.info(f"  - Removed index columns")
        self.logger.info(f"  - Filtered to Brazilian users only")
        self.logger.info(f"  - Standardized metric flags to integers")
        self.logger.info(f"  - Handled missing values in flags")
        self.logger.info(f"  - Validated first_login_dt")
        self.logger.info(f"  - Filtered to 2022 date range")
        
        return onboarding_df
    
    def validate_data_consistency(self, users_df, transactions_df, onboarding_df):
        """
        Validate data consistency across datasets
        
        Args:
            users_df: Cleaned users dataset
            transactions_df: Cleaned transactions dataset
            onboarding_df: Cleaned onboarding dataset
        
        Returns:
            dict: Validation results
        """
        self.logger.info("Validating data consistency across datasets...")
        
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
        
        self.logger.info(f"Data consistency validation results:")
        self.logger.info(f"  - Users dataset: {users_ids:,} unique users")
        self.logger.info(f"  - Transactions dataset: {transactions_ids:,} unique users")
        self.logger.info(f"  - Onboarding dataset: {onboarding_ids:,} unique users")
        self.logger.info(f"  - Users-Transactions overlap: {users_transactions_overlap:,}")
        self.logger.info(f"  - Users-Onboarding overlap: {users_onboarding_overlap:,}")
        self.logger.info(f"  - Transactions-Onboarding overlap: {transactions_onboarding_overlap:,}")
        
        return validation_results
    
    def run_preprocessing_pipeline(self):
        """
        Run the complete preprocessing pipeline
        
        Returns:
            dict: Dictionary containing all cleaned datasets
        """
        self.logger.info("Starting complete preprocessing pipeline...")
        
        # Preprocess each dataset
        clean_users_df = self.preprocess_users_dataset()
        clean_transactions_df = self.preprocess_transactions_dataset()
        clean_onboarding_df = self.preprocess_onboarding_dataset()
        
        # Validate data consistency
        validation_results = self.validate_data_consistency(
            clean_users_df, clean_transactions_df, clean_onboarding_df
        )
        
        # Create output directory if it doesn't exist
        os.makedirs(os.path.dirname(PROCESSED_DATA_PATHS["clean_users"]), exist_ok=True)
        
        # Save cleaned datasets
        write_dataframe_to_parquet(clean_users_df, PROCESSED_DATA_PATHS["clean_users"])
        write_dataframe_to_parquet(clean_transactions_df, PROCESSED_DATA_PATHS["clean_transactions"])
        write_dataframe_to_parquet(clean_onboarding_df, PROCESSED_DATA_PATHS["clean_onboarding"])
        
        self.logger.info("Preprocessing pipeline completed successfully!")
        
        return {
            "users": clean_users_df,
            "transactions": clean_transactions_df,
            "onboarding": clean_onboarding_df,
            "validation": validation_results
        }

def main():
    """
    Main function to run the preprocessing pipeline
    """
    print("="*60)
    print("FINTECH ETL - DATA PREPROCESSING AND SANITIZATION")
    print("="*60)
    
    # Create Spark session
    spark = create_spark_session("Data_Preprocessing")
    
    try:
        # Initialize preprocessor
        preprocessor = DataPreprocessor(spark)
        
        # Run preprocessing pipeline
        results = preprocessor.run_preprocessing_pipeline()
        
        # Show sample of cleaned data
        print("\n" + "="*60)
        print("CLEANED DATA SAMPLES")
        print("="*60)
        
        for name, df in results.items():
            if name != "validation":
                show_sample_data(df, f"CLEAN_{name}")
        
        print("\n" + "="*60)
        print("PREPROCESSING COMPLETED SUCCESSFULLY!")
        print("="*60)
        
    except Exception as e:
        print(f"Error during preprocessing: {e}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()