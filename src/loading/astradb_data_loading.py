"""
AstraDB Data Loading Pipeline for Fintech ETL

This module handles loading the corrected transformed analytics data 
directly to AstraDB using Spark Cassandra connector.

Key Features:
- Direct AstraDB connection using secure connect bundle
- Automatic table creation with proper schemas
- Optimized for AstraDB cloud environment
- Fallback to Parquet if connection fails
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from config.spark_config import create_spark_session, get_cassandra_config
from config.etl_config import (
    PROCESSED_DATA_PATHS, 
    CASSANDRA_CONFIG,
    USER_SEGMENTS
)
from src.utils import (
    setup_logging, 
    read_parquet_file
)
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, count, sum as spark_sum, avg, max as spark_max, min as spark_min,
    date_format, concat, desc, asc, round as spark_round
)
from pyspark.sql.types import DoubleType
import logging

class AstraDBDataLoader:
    """
    AstraDB data loading and analytics integration class
    """
    
    def __init__(self, spark_session: SparkSession):
        self.spark = spark_session
        self.logger = setup_logging()
        self.cassandra_config = get_cassandra_config(use_astradb=True)
        
    def load_transformed_datasets(self):
        """
        Load all transformed datasets from parquet files
        """
        self.logger.info("Loading transformed datasets for AstraDB...")
        
        datasets = {}
        datasets["user_segments"] = read_parquet_file(self.spark, PROCESSED_DATA_PATHS["user_metrics"])
        datasets["comprehensive_metrics"] = read_parquet_file(self.spark, PROCESSED_DATA_PATHS["final_analytics"])
        
        # Log counts
        segments_count = datasets["user_segments"].count()
        metrics_count = datasets["comprehensive_metrics"].count()
        
        self.logger.info(f"✅ Transformed datasets loaded for AstraDB:")
        self.logger.info(f"  - User segments: {segments_count:,} users with transactions")
        self.logger.info(f"  - Comprehensive metrics: {metrics_count:,} total users")
        
        return datasets
    
    def create_aggregated_funnel_analysis(self, comprehensive_metrics_df):
        """
        Create aggregated funnel analysis data for AstraDB
        """
        self.logger.info("Creating aggregated funnel analysis for AstraDB...")
        
        # Overall funnel analysis
        overall_funnel = comprehensive_metrics_df.agg(
            count("*").alias("total_users"),
            spark_sum(when(col("drop_flag") == 0, 1).otherwise(0)).alias("stage_2_returned"),
            spark_sum("activation_validated").alias("stage_3_activated"),
            spark_sum("setup_achieved").alias("stage_4_setup"),
            spark_sum("habit_validated").alias("stage_5_habit")
        ).withColumn("dimension_type", lit("overall")
        ).withColumn("dimension_value", lit("all")
        ).withColumn("stage_1_registered", col("total_users"))
        
        # Funnel by user segment
        segment_funnel = comprehensive_metrics_df.groupBy("user_segment").agg(
            count("*").alias("total_users"),
            spark_sum(when(col("drop_flag") == 0, 1).otherwise(0)).alias("stage_2_returned"),
            spark_sum("activation_validated").alias("stage_3_activated"),
            spark_sum("setup_achieved").alias("stage_4_setup"),
            spark_sum("habit_validated").alias("stage_5_habit")
        ).withColumn("dimension_type", lit("user_segment")
        ).withColumn("dimension_value", col("user_segment")
        ).withColumn("stage_1_registered", col("total_users")
        ).drop("user_segment")
        
        # Funnel by first login month
        month_funnel = comprehensive_metrics_df.groupBy("first_login_month").agg(
            count("*").alias("total_users"),
            spark_sum(when(col("drop_flag") == 0, 1).otherwise(0)).alias("stage_2_returned"),
            spark_sum("activation_validated").alias("stage_3_activated"),
            spark_sum("setup_achieved").alias("stage_4_setup"),
            spark_sum("habit_validated").alias("stage_5_habit")
        ).withColumn("dimension_type", lit("month")
        ).withColumn("dimension_value", col("first_login_month").cast("string")
        ).withColumn("stage_1_registered", col("total_users")
        ).drop("first_login_month")
        
        # Union all funnel analyses
        funnel_analysis = overall_funnel.unionByName(segment_funnel).unionByName(month_funnel)
        
        self.logger.info("✅ Aggregated funnel analysis created for AstraDB")
        return funnel_analysis
    
    def create_monthly_metrics(self, comprehensive_metrics_df):
        """
        Create monthly aggregated metrics by user segment for AstraDB
        """
        self.logger.info("Creating monthly metrics for AstraDB...")
        
        monthly_metrics = comprehensive_metrics_df.groupBy("first_login_month", "user_segment").agg(
            count("*").alias("total_users"),
            spark_sum(when(col("drop_flag") == 0, 1).otherwise(0)).alias("returned_users"),
            spark_sum("activation_validated").alias("activated_users"),
            spark_sum("setup_achieved").alias("setup_completed_users"),
            spark_sum("habit_validated").alias("habit_achieved_users")
        ).withColumn("year_month", 
            concat(lit("2022-"), 
                   when(col("first_login_month") < 10, 
                        concat(lit("0"), col("first_login_month")))
                   .otherwise(col("first_login_month").cast("string")))
        ).withColumn("drop_rate",
            spark_round((lit(1.0) - col("returned_users").cast(DoubleType()) / col("total_users").cast(DoubleType())) * 100, 2)
        ).withColumn("activation_rate",
            spark_round(col("activated_users").cast(DoubleType()) / col("total_users").cast(DoubleType()) * 100, 2)
        ).withColumn("setup_rate",
            spark_round(col("setup_completed_users").cast(DoubleType()) / col("total_users").cast(DoubleType()) * 100, 2)
        ).withColumn("habit_rate",
            spark_round(col("habit_achieved_users").cast(DoubleType()) / col("total_users").cast(DoubleType()) * 100, 2)
        ).select(
            "year_month", "user_segment", "total_users", "returned_users", 
            "activated_users", "setup_completed_users", "habit_achieved_users",
            "drop_rate", "activation_rate", "setup_rate", "habit_rate"
        )
        
        self.logger.info("✅ Monthly metrics created for AstraDB")
        return monthly_metrics
    
    def create_weekly_metrics(self, comprehensive_metrics_df):
        """
        Create weekly aggregated metrics by user segment for AstraDB
        """
        self.logger.info("Creating weekly metrics for AstraDB...")
        
        weekly_metrics = comprehensive_metrics_df.groupBy("first_login_week", "user_segment").agg(
            count("*").alias("total_users"),
            spark_sum(when(col("drop_flag") == 0, 1).otherwise(0)).alias("returned_users"),
            spark_sum("activation_validated").alias("activated_users"),
            spark_sum("setup_achieved").alias("setup_completed_users"),
            spark_sum("habit_validated").alias("habit_achieved_users")
        ).withColumn("year_week",
            concat(lit("2022-W"), 
                   when(col("first_login_week") < 10, 
                        concat(lit("0"), col("first_login_week")))
                   .otherwise(col("first_login_week").cast("string")))
        ).withColumn("drop_rate",
            spark_round((lit(1.0) - col("returned_users").cast(DoubleType()) / col("total_users").cast(DoubleType())) * 100, 2)
        ).withColumn("activation_rate",
            spark_round(col("activated_users").cast(DoubleType()) / col("total_users").cast(DoubleType()) * 100, 2)
        ).withColumn("setup_rate",
            spark_round(col("setup_completed_users").cast(DoubleType()) / col("total_users").cast(DoubleType()) * 100, 2)
        ).withColumn("habit_rate",
            spark_round(col("habit_achieved_users").cast(DoubleType()) / col("total_users").cast(DoubleType()) * 100, 2)
        ).select(
            "year_week", "user_segment", "total_users", "returned_users",
            "activated_users", "setup_completed_users", "habit_achieved_users", 
            "drop_rate", "activation_rate", "setup_rate", "habit_rate"
        )
        
        self.logger.info("✅ Weekly metrics created for AstraDB")
        return weekly_metrics
    
    def write_to_astradb(self, df, table_name, mode="overwrite"):
        """
        Write DataFrame to AstraDB with fallback mechanism
        """
        try:
            keyspace = self.cassandra_config["keyspace"]
            
            self.logger.info(f"🔄 Writing {df.count()} rows to AstraDB table: {keyspace}.{table_name}")
            
            # Write to AstraDB using Spark Cassandra connector
            df.write \
                .format("org.apache.spark.sql.cassandra") \
                .options(table=table_name, keyspace=keyspace) \
                .mode(mode) \
                .option("confirm.truncate", "true") \
                .save()
            
            self.logger.info(f"✅ Successfully wrote to AstraDB table: {keyspace}.{table_name}")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Failed to write to AstraDB table {table_name}: {e}")
            
            # Fallback to parquet
            fallback_path = f"processed_data/astradb_fallback_{table_name}.parquet"
            df.write.mode(mode).parquet(fallback_path)
            self.logger.info(f"💾 Fallback: Saved to {fallback_path}")
            return False
    
    def load_user_segments_table(self, user_segments_df):
        """
        Load user segments data to AstraDB
        """
        self.logger.info("Loading user segments table to AstraDB...")
        
        # Prepare data for AstraDB storage
        astradb_user_segments = user_segments_df.select(
            "user_id",
            "user_segment", 
            "total_transactions",
            "unique_transaction_types",
            "transaction_days",
            "first_transaction_dt",
            "last_transaction_dt"
        )
        
        return self.write_to_astradb(astradb_user_segments, "user_segments")
    
    def load_user_metrics_table(self, comprehensive_metrics_df):
        """
        Load comprehensive user metrics to AstraDB
        """
        self.logger.info("Loading user metrics table to AstraDB...")
        
        # Prepare data for AstraDB storage
        astradb_user_metrics = comprehensive_metrics_df.select(
            "user_id",
            "first_login_dt",
            "week_year",
            "user_segment",
            "total_transactions",
            "transaction_days",
            "drop_flag",
            col("return").alias("return_flag"),
            "return_dt",
            "activation_validated",
            "activacion_dt",
            "habit_validated", 
            "total_transactions_period",
            "unique_transaction_days",
            "setup_achieved",
            "setup_dt",
            "onboarding_end_dt",
            "first_login_month",
            "first_login_week"
        )
        
        return self.write_to_astradb(astradb_user_metrics, "user_metrics")
    
    def run_astradb_loading_pipeline(self):
        """
        Run the complete AstraDB data loading pipeline
        """
        self.logger.info("🚀 Starting AstraDB data loading pipeline...")
        
        # Load transformed datasets
        datasets = self.load_transformed_datasets()
        
        # Track success for each table
        results = {}
        
        # Load core tables to AstraDB
        results["user_segments"] = self.load_user_segments_table(datasets["user_segments"])
        results["user_metrics"] = self.load_user_metrics_table(datasets["comprehensive_metrics"])
        
        # Create and load aggregated analysis tables
        funnel_analysis = self.create_aggregated_funnel_analysis(datasets["comprehensive_metrics"])
        results["funnel_analysis"] = self.write_to_astradb(funnel_analysis, "funnel_analysis")
        
        monthly_metrics = self.create_monthly_metrics(datasets["comprehensive_metrics"])
        results["monthly_metrics"] = self.write_to_astradb(monthly_metrics, "monthly_metrics")
        
        weekly_metrics = self.create_weekly_metrics(datasets["comprehensive_metrics"])
        results["weekly_metrics"] = self.write_to_astradb(weekly_metrics, "weekly_metrics")
        
        # Calculate success rate
        successful_tables = sum(1 for success in results.values() if success)
        total_tables = len(results)
        
        if successful_tables == total_tables:
            self.logger.info("🎉 AstraDB data loading pipeline completed successfully!")
        else:
            self.logger.warning(f"⚠️  AstraDB loading partially successful: {successful_tables}/{total_tables} tables")
        
        return results

def main():
    """
    Main function to run the AstraDB data loading pipeline
    """
    print("=" * 70)
    print("FINTECH ETL - ASTRADB DATA LOADING")
    print("=" * 70)
    print("🌟 LOADING: Data directly to AstraDB cloud database")
    print("📊 STORING: Analytics data with cloud persistence")
    print("=" * 70)
    
    # Create Spark session with AstraDB configuration
    spark = create_spark_session("AstraDB_Data_Loading", use_astradb=True)
    
    try:
        # Initialize AstraDB loader
        loader = AstraDBDataLoader(spark)
        
        # Run AstraDB loading pipeline
        results = loader.run_astradb_loading_pipeline()
        
        print("\n" + "=" * 70)
        print("✅ ASTRADB LOADING RESULTS")
        print("=" * 70)
        
        for table, success in results.items():
            status_icon = "✅" if success else "❌"
            status_text = "SUCCESS" if success else "FAILED (fallback used)"
            print(f"{status_icon} {table}: {status_text}")
        
        successful_tables = sum(1 for success in results.values() if success)
        total_tables = len(results)
        
        print(f"\n📊 Summary: {successful_tables}/{total_tables} tables loaded to AstraDB")
        
        if successful_tables == total_tables:
            print("\n🎉 ALL DATA SUCCESSFULLY LOADED TO ASTRADB!")
            print("🔍 You can now view your data in the AstraDB console:")
            print("   https://astra.datastax.com/")
        else:
            print(f"\n⚠️  Partial success - check fallback Parquet files for failed tables")
        
        print("\n" + "=" * 70)
        print("✅ ASTRADB DATA LOADING COMPLETED!")
        print("=" * 70)
        
        return results
        
    except Exception as e:
        print(f"❌ Error during AstraDB data loading: {e}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()