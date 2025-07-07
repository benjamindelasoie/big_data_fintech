"""
Data Transformation and Metrics Calculation for Fintech ETL Pipeline

This module handles the transformation of cleaned data into analytics-ready datasets.
It implements user segmentation logic and calculates the four key business metrics.

Key Business Logic:
1. User Segmentation: Individuals, Sellers, Unknown (based on transaction behavior)
2. Metrics Calculation: Drop, Activation, Habit, Setup
3. Onboarding Period Analysis (30 days from first login)
4. A/B Test Results Analysis (data already contains test results)
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from config.spark_config import create_spark_session
from config.etl_config import (
    PROCESSED_DATA_PATHS, 
    TRANSACTION_TYPES, 
    USER_SEGMENTS,
    ONBOARDING_PERIOD_DAYS,
    HABIT_RULES
)
from src.utils import (
    setup_logging, 
    write_dataframe_to_parquet,
    read_parquet_file,
    show_sample_data
)
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, when, lit, count, countDistinct, sum as spark_sum, max as spark_max, min as spark_min,
    datediff, date_add, desc, asc, first, last, collect_list, size, array_distinct,
    to_date, date_format, dayofweek, weekofyear, month, year,
    lag, lead, row_number, rank, dense_rank,
    coalesce, isnan, isnull, broadcast
)
from pyspark.sql.types import IntegerType, StringType, DoubleType, DateType, TimestampType
from datetime import datetime, timedelta
import logging

class DataTransformer:
    """
    Data transformation and metrics calculation class
    """
    
    def __init__(self, spark_session: SparkSession):
        self.spark = spark_session
        self.logger = setup_logging()
        
    def load_clean_datasets(self):
        """
        Load all cleaned datasets from parquet files
        
        Returns:
            dict: Dictionary containing all cleaned datasets
        """
        self.logger.info("Loading cleaned datasets...")
        
        datasets = {}
        datasets["users"] = read_parquet_file(self.spark, PROCESSED_DATA_PATHS["clean_users"])
        datasets["transactions"] = read_parquet_file(self.spark, PROCESSED_DATA_PATHS["clean_transactions"])
        datasets["onboarding"] = read_parquet_file(self.spark, PROCESSED_DATA_PATHS["clean_onboarding"])
        
        self.logger.info("All cleaned datasets loaded successfully")
        return datasets
    
    def classify_user_segments(self, transactions_df):
        """
        Classify users into segments based on their transaction behavior
        
        Business Logic:
        - Individuals: Users who only make payment transactions (types 1-7)
        - Sellers: Users who make collection transactions (types 8-9) 
        - Unknown: Users with no transactions (handled later in join)
        
        Args:
            transactions_df: Clean transactions DataFrame
            
        Returns:
            DataFrame: User segments with columns [user_id, user_segment, transaction_summary]
        """
        self.logger.info("Starting user segmentation based on transaction behavior...")
        
        # Get payment transactions (types 1-7)
        payment_users = transactions_df.filter(
            col("type").isin(TRANSACTION_TYPES["PAYMENT"])
        ).select("user_id").distinct().withColumn("has_payments", lit(1))
        
        # Get collection transactions (types 8-9)
        collection_users = transactions_df.filter(
            col("type").isin(TRANSACTION_TYPES["COLLECTION"])
        ).select("user_id").distinct().withColumn("has_collections", lit(1))
        
        # Join to get complete transaction behavior picture
        user_transaction_behavior = payment_users.join(
            collection_users, on="user_id", how="outer"
        ).fillna(0)
        
        # Classify users based on transaction types
        user_segments = user_transaction_behavior.withColumn("user_segment",
            when(
                (col("has_payments") == 1) & (col("has_collections") == 1), 
                lit("SELLERS")  # Users with both payments and collections
            ).when(
                (col("has_payments") == 1) & (col("has_collections") == 0), 
                lit("INDIVIDUALS")  # Users with only payments
            ).when(
                (col("has_payments") == 0) & (col("has_collections") == 1), 
                lit("SELLERS")  # Users with only collections (edge case)
            ).otherwise(lit("UNKNOWN"))  # Should not happen in this dataset
        ).select("user_id", "user_segment")
        
        # Add transaction count summary
        transaction_summary = transactions_df.groupBy("user_id").agg(
            count("*").alias("total_transactions"),
            countDistinct("type").alias("unique_transaction_types"),
            countDistinct(to_date("transaction_dt")).alias("transaction_days"),
            spark_min("transaction_dt").alias("first_transaction_dt"),
            spark_max("transaction_dt").alias("last_transaction_dt")
        )
        
        # Join segments with transaction summary
        user_segments_enriched = user_segments.join(
            transaction_summary, on="user_id", how="left"
        )
        
        segments_count = user_segments_enriched.groupBy("user_segment").count().collect()
        self.logger.info("User segmentation completed:")
        for row in segments_count:
            self.logger.info(f"  - {row['user_segment']}: {row['count']:,} users")
        
        return user_segments_enriched
    
    def calculate_drop_metric(self, onboarding_df, transactions_df):
        """
        Calculate Drop metric: Users who don't return after viewing home page on registration day
        
        Business Logic:
        - Drop = Users who don't have any activity after their first_login_dt on the same day
        - We use the 'return' flag from onboarding dataset as indicator
        - Drop rate = (1 - return_flag) * 100
        
        Args:
            onboarding_df: Clean onboarding DataFrame
            transactions_df: Clean transactions DataFrame
            
        Returns:
            DataFrame: Drop metrics per user
        """
        self.logger.info("Calculating Drop metric...")
        
        # Drop metric is inverse of return flag
        drop_metrics = onboarding_df.select(
            "user_id",
            "first_login_dt",
            "return",
            "return_dt"
        ).withColumn("drop_flag", 
            when(col("return") == 1, lit(0)).otherwise(lit(1))
        )
        
        drop_count = drop_metrics.filter(col("drop_flag") == 1).count()
        total_users = drop_metrics.count()
        drop_rate = (drop_count / total_users) * 100
        
        self.logger.info(f"Drop metric calculated: {drop_count:,} users dropped out of {total_users:,} ({drop_rate:.2f}%)")
        
        return drop_metrics
    
    def calculate_activation_metric(self, onboarding_df, transactions_df):
        """
        Calculate Activation metric: Users who perform first transactional action
        
        Business Logic:
        - Activation = Users who have at least one transaction during onboarding period
        - We can use the 'activacion' flag from onboarding dataset
        - Cross-validate with actual transaction data
        
        Args:
            onboarding_df: Clean onboarding DataFrame
            transactions_df: Clean transactions DataFrame
            
        Returns:
            DataFrame: Activation metrics per user
        """
        self.logger.info("Calculating Activation metric...")
        
        # Get users with transactions (cross-validation)
        users_with_transactions = transactions_df.select("user_id").distinct().withColumn("has_transactions", lit(1))
        
        # Join with onboarding data
        activation_metrics = onboarding_df.select(
            "user_id",
            "first_login_dt", 
            "activacion",
            "activacion_dt"
        ).join(
            users_with_transactions, on="user_id", how="left"
        ).fillna(0, subset=["has_transactions"])
        
        # Validate activation flag against actual transactions
        activation_metrics = activation_metrics.withColumn("activation_validated",
            when(
                (col("activacion") == 1) & (col("has_transactions") == 1), lit(1)
            ).when(
                (col("activacion") == 0) & (col("has_transactions") == 0), lit(0)
            ).otherwise(col("activacion"))  # Trust the flag if there's discrepancy
        )
        
        activated_count = activation_metrics.filter(col("activation_validated") == 1).count()
        total_users = activation_metrics.count()
        activation_rate = (activated_count / total_users) * 100
        
        self.logger.info(f"Activation metric calculated: {activated_count:,} users activated out of {total_users:,} ({activation_rate:.2f}%)")
        
        return activation_metrics
    
    def calculate_habit_metric(self, onboarding_df, transactions_df, user_segments_df):
        """
        Calculate Habit metric: Users who complete 5 transactions based on their segment
        
        Business Logic:
        - Individuals: 5 transactions on 5 different days within 30-day onboarding
        - Sellers: 5 collection transactions (types 8-9) within 30-day onboarding  
        - Unknown: Apply individuals rule (as per teacher clarification)
        
        Args:
            onboarding_df: Clean onboarding DataFrame
            transactions_df: Clean transactions DataFrame
            user_segments_df: User segments DataFrame
            
        Returns:
            DataFrame: Habit metrics per user
        """
        self.logger.info("Calculating Habit metric...")
        
        # Join onboarding with user segments to get segment information
        onboarding_with_segments = onboarding_df.join(
            user_segments_df.select("user_id", "user_segment"), 
            on="user_id", 
            how="left"
        ).fillna("UNKNOWN", subset=["user_segment"])
        
        # Calculate onboarding end date (30 days from first login)
        onboarding_with_periods = onboarding_with_segments.withColumn(
            "onboarding_end_dt", 
            date_add(col("first_login_dt"), ONBOARDING_PERIOD_DAYS)
        )
        
        # Get transactions within onboarding period for each user
        onboarding_transactions = onboarding_with_periods.join(
            transactions_df, on="user_id", how="left"
        ).filter(
            col("transaction_dt").isNotNull() &
            (col("transaction_dt") >= col("first_login_dt")) &
            (col("transaction_dt") <= col("onboarding_end_dt"))
        )
        
        # Calculate habit for INDIVIDUALS and UNKNOWN (same rule)
        individuals_habit = onboarding_transactions.filter(
            col("user_segment").isin(["INDIVIDUALS", "UNKNOWN"])
        ).groupBy("user_id", "user_segment", "first_login_dt", "habito").agg(
            count("*").alias("total_transactions_period"),
            countDistinct(to_date("transaction_dt")).alias("unique_transaction_days")
        ).withColumn("habit_achieved_calculated",
            when(
                (col("total_transactions_period") >= 5) & 
                (col("unique_transaction_days") >= 5), 
                lit(1)
            ).otherwise(lit(0))
        )
        
        # Calculate habit for SELLERS (5 collection transactions)
        sellers_habit = onboarding_transactions.filter(
            col("user_segment") == "SELLERS"
        ).filter(
            col("type").isin(TRANSACTION_TYPES["COLLECTION"])  # Only collection transactions
        ).groupBy("user_id", "user_segment", "first_login_dt", "habito").agg(
            count("*").alias("total_collection_transactions")
        ).withColumn("habit_achieved_calculated",
            when(col("total_collection_transactions") >= 5, lit(1)).otherwise(lit(0))
        ).withColumn("total_transactions_period", col("total_collection_transactions")
        ).withColumn("unique_transaction_days", lit(None).cast(IntegerType()))
        
        # Union both calculations
        habit_metrics = individuals_habit.unionByName(
            sellers_habit, allowMissingColumns=True
        )
        
        # Handle users without transactions (they don't achieve habit)
        all_users_with_segments = onboarding_with_periods.select(
            "user_id", "user_segment", "first_login_dt", "habito"
        )
        
        habit_metrics_complete = all_users_with_segments.join(
            habit_metrics, 
            on=["user_id", "user_segment", "first_login_dt", "habito"], 
            how="left"
        ).fillna(0, subset=["habit_achieved_calculated", "total_transactions_period"])
        
        # Validate against existing habit flag
        habit_metrics_final = habit_metrics_complete.withColumn("habit_validated",
            when(
                (col("habito") == 1) & (col("habit_achieved_calculated") == 1), lit(1)
            ).when(
                (col("habito") == 0) & (col("habit_achieved_calculated") == 0), lit(0)
            ).otherwise(col("habito"))  # Trust the existing flag if there's discrepancy
        )
        
        # Log results by segment
        habit_summary = habit_metrics_final.groupBy("user_segment").agg(
            count("*").alias("total_users"),
            spark_sum("habit_validated").alias("habit_achieved")
        ).collect()
        
        self.logger.info("Habit metric calculated by segment:")
        for row in habit_summary:
            rate = (row['habit_achieved'] / row['total_users']) * 100
            self.logger.info(f"  - {row['user_segment']}: {row['habit_achieved']:,}/{row['total_users']:,} ({rate:.2f}%)")
        
        return habit_metrics_final
    
    def calculate_setup_metric(self, onboarding_df):
        """
        Calculate Setup metric: Users who performed at least one setup action
        
        Business Logic:
        - Setup = Users who completed at least one setup action (flag = 1)
        - Examples: add credit card, activate PIX, activate account yields, etc.
        
        Args:
            onboarding_df: Clean onboarding DataFrame
            
        Returns:
            DataFrame: Setup metrics per user
        """
        self.logger.info("Calculating Setup metric...")
        
        setup_metrics = onboarding_df.select(
            "user_id",
            "first_login_dt",
            "setup",
            "setup_dt"
        ).withColumn("setup_achieved", col("setup"))
        
        setup_count = setup_metrics.filter(col("setup_achieved") == 1).count()
        total_users = setup_metrics.count()
        setup_rate = (setup_count / total_users) * 100
        
        self.logger.info(f"Setup metric calculated: {setup_count:,} users completed setup out of {total_users:,} ({setup_rate:.2f}%)")
        
        return setup_metrics
    
    def create_comprehensive_user_metrics(self, datasets, user_segments_df):
        """
        Create a comprehensive dataset with all user metrics
        
        Args:
            datasets: Dictionary containing all cleaned datasets
            user_segments_df: User segments DataFrame
            
        Returns:
            DataFrame: Comprehensive user metrics dataset
        """
        self.logger.info("Creating comprehensive user metrics dataset...")
        
        # Calculate all metrics
        drop_metrics = self.calculate_drop_metric(datasets["onboarding"], datasets["transactions"])
        activation_metrics = self.calculate_activation_metric(datasets["onboarding"], datasets["transactions"])
        habit_metrics = self.calculate_habit_metric(datasets["onboarding"], datasets["transactions"], user_segments_df)
        setup_metrics = self.calculate_setup_metric(datasets["onboarding"])
        
        # Start with base onboarding data
        comprehensive_metrics = datasets["onboarding"].select(
            "user_id",
            "first_login_dt",
            "week_year"
        )
        
        # Join with user segments
        comprehensive_metrics = comprehensive_metrics.join(
            user_segments_df.select("user_id", "user_segment", "total_transactions", "transaction_days"),
            on="user_id",
            how="left"
        ).fillna("UNKNOWN", subset=["user_segment"])
        
        # Join with all metrics
        comprehensive_metrics = comprehensive_metrics.join(
            drop_metrics.select("user_id", "drop_flag", "return", "return_dt"),
            on="user_id",
            how="left"
        ).join(
            activation_metrics.select("user_id", "activation_validated", "activacion_dt"),
            on="user_id", 
            how="left"
        ).join(
            habit_metrics.select("user_id", "habit_validated", "total_transactions_period", "unique_transaction_days"),
            on="user_id",
            how="left"
        ).join(
            setup_metrics.select("user_id", "setup_achieved", "setup_dt"),
            on="user_id",
            how="left"
        )
        
        # Add derived metrics
        comprehensive_metrics = comprehensive_metrics.withColumn("onboarding_end_dt",
            date_add(col("first_login_dt"), ONBOARDING_PERIOD_DAYS)
        ).withColumn("first_login_month", 
            month(col("first_login_dt"))
        ).withColumn("first_login_week",
            weekofyear(col("first_login_dt"))
        )
        
        # Fill nulls for users without transactions
        comprehensive_metrics = comprehensive_metrics.fillna(0, subset=[
            "drop_flag", "activation_validated", "habit_validated", "setup_achieved",
            "total_transactions", "transaction_days", "total_transactions_period"
        ])
        
        self.logger.info(f"Comprehensive user metrics dataset created with {comprehensive_metrics.count():,} users")
        
        return comprehensive_metrics
    
    def create_funnel_analysis_dataset(self, comprehensive_metrics_df):
        """
        Create a dataset for funnel analysis showing user progression through onboarding stages
        
        Args:
            comprehensive_metrics_df: Comprehensive user metrics DataFrame
            
        Returns:
            DataFrame: Funnel analysis dataset
        """
        self.logger.info("Creating funnel analysis dataset...")
        
        # Define funnel stages
        funnel_metrics = comprehensive_metrics_df.withColumn("stage_1_registered", lit(1)
        ).withColumn("stage_2_returned",
            when(col("drop_flag") == 0, lit(1)).otherwise(lit(0))
        ).withColumn("stage_3_activated", 
            col("activation_validated")
        ).withColumn("stage_4_setup",
            col("setup_achieved")
        ).withColumn("stage_5_habit",
            col("habit_validated")
        )
        
        # Calculate funnel progression
        funnel_summary = funnel_metrics.agg(
            count("*").alias("total_users"),
            spark_sum("stage_1_registered").alias("registered"),
            spark_sum("stage_2_returned").alias("returned"),
            spark_sum("stage_3_activated").alias("activated"),
            spark_sum("stage_4_setup").alias("setup_completed"),
            spark_sum("stage_5_habit").alias("habit_achieved")
        )
        
        # Log funnel results
        funnel_row = funnel_summary.collect()[0]
        self.logger.info("Onboarding funnel analysis:")
        self.logger.info(f"  1. Registered: {funnel_row['registered']:,} (100.0%)")
        self.logger.info(f"  2. Returned: {funnel_row['returned']:,} ({funnel_row['returned']/funnel_row['registered']*100:.1f}%)")
        self.logger.info(f"  3. Activated: {funnel_row['activated']:,} ({funnel_row['activated']/funnel_row['registered']*100:.1f}%)")
        self.logger.info(f"  4. Setup: {funnel_row['setup_completed']:,} ({funnel_row['setup_completed']/funnel_row['registered']*100:.1f}%)")
        self.logger.info(f"  5. Habit: {funnel_row['habit_achieved']:,} ({funnel_row['habit_achieved']/funnel_row['registered']*100:.1f}%)")
        
        return funnel_metrics
    
    def run_transformation_pipeline(self):
        """
        Run the complete transformation pipeline
        
        Returns:
            dict: Dictionary containing all transformed datasets
        """
        self.logger.info("Starting complete transformation pipeline...")
        
        # Load cleaned datasets
        datasets = self.load_clean_datasets()
        
        # Classify user segments
        user_segments = self.classify_user_segments(datasets["transactions"])
        
        # Create comprehensive user metrics
        comprehensive_metrics = self.create_comprehensive_user_metrics(datasets, user_segments)
        
        # Create funnel analysis dataset  
        funnel_analysis = self.create_funnel_analysis_dataset(comprehensive_metrics)
        
        # Save transformed datasets
        write_dataframe_to_parquet(user_segments, PROCESSED_DATA_PATHS["user_metrics"])
        write_dataframe_to_parquet(comprehensive_metrics, PROCESSED_DATA_PATHS["final_analytics"])
        
        self.logger.info("Transformation pipeline completed successfully!")
        
        return {
            "user_segments": user_segments,
            "comprehensive_metrics": comprehensive_metrics,
            "funnel_analysis": funnel_analysis
        }

def main():
    """
    Main function to run the transformation pipeline
    """
    print("="*60)
    print("FINTECH ETL - DATA TRANSFORMATION AND METRICS CALCULATION")
    print("="*60)
    
    # Create Spark session
    spark = create_spark_session("Data_Transformation")
    
    try:
        # Initialize transformer
        transformer = DataTransformer(spark)
        
        # Run transformation pipeline
        results = transformer.run_transformation_pipeline()
        
        # Show sample of transformed data
        print("\n" + "="*60)
        print("TRANSFORMED DATA SAMPLES")
        print("="*60)
        
        for name, df in results.items():
            show_sample_data(df, f"TRANSFORMED_{name}")
        
        print("\n" + "="*60)
        print("TRANSFORMATION COMPLETED SUCCESSFULLY!")
        print("="*60)
        
    except Exception as e:
        print(f"Error during transformation: {e}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()