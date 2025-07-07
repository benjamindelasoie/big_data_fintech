"""
Common utilities for the ETL pipeline
"""
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, count, sum as spark_sum, when, isnan, isnull, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType

def setup_logging(log_level: str = "INFO") -> logging.Logger:
    """
    Set up logging configuration
    
    Args:
        log_level (str): Logging level
    
    Returns:
        logging.Logger: Configured logger
    """
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    return logging.getLogger(__name__)

def validate_dataframe_schema(df: DataFrame, expected_columns: List[str]) -> bool:
    """
    Validate that DataFrame has expected columns
    
    Args:
        df (DataFrame): Input DataFrame
        expected_columns (List[str]): List of expected column names
    
    Returns:
        bool: True if all expected columns are present
    """
    df_columns = set(df.columns)
    expected_columns_set = set(expected_columns)
    
    missing_columns = expected_columns_set - df_columns
    if missing_columns:
        logging.error(f"Missing columns: {missing_columns}")
        return False
    
    return True

def calculate_data_quality_metrics(df: DataFrame) -> Dict:
    """
    Calculate data quality metrics for a DataFrame
    
    Args:
        df (DataFrame): Input DataFrame
    
    Returns:
        Dict: Data quality metrics
    """
    total_rows = df.count()
    
    if total_rows == 0:
        return {"total_rows": 0, "columns": {}}
    
    quality_metrics = {
        "total_rows": total_rows,
        "columns": {}
    }
    
    for column in df.columns:
        null_count = df.filter(col(column).isNull() | isnan(col(column))).count()
        missing_percentage = (null_count / total_rows) * 100
        
        quality_metrics["columns"][column] = {
            "null_count": null_count,
            "missing_percentage": round(missing_percentage, 2),
            "non_null_count": total_rows - null_count
        }
    
    return quality_metrics

def print_data_quality_report(quality_metrics: Dict, dataset_name: str):
    """
    Print a formatted data quality report
    
    Args:
        quality_metrics (Dict): Data quality metrics
        dataset_name (str): Name of the dataset
    """
    print(f"\n=== DATA QUALITY REPORT: {dataset_name.upper()} ===")
    print(f"Total rows: {quality_metrics['total_rows']:,}")
    print(f"Total columns: {len(quality_metrics['columns'])}")
    
    print("\nColumn-wise quality metrics:")
    for column, metrics in quality_metrics['columns'].items():
        print(f"  {column}:")
        print(f"    - Null count: {metrics['null_count']:,}")
        print(f"    - Missing %: {metrics['missing_percentage']:.2f}%")
        print(f"    - Non-null count: {metrics['non_null_count']:,}")
    
    print("=" * 50)

def filter_brazilian_users(df: DataFrame, user_id_column: str = "user_id") -> DataFrame:
    """
    Filter users with Brazilian IDs (MLB prefix)
    
    Args:
        df (DataFrame): Input DataFrame
        user_id_column (str): Name of the user ID column
    
    Returns:
        DataFrame: Filtered DataFrame with only Brazilian users
    """
    return df.filter(col(user_id_column).startswith("MLB"))

def calculate_onboarding_period(first_login_date: datetime, period_days: int = 30) -> Tuple[datetime, datetime]:
    """
    Calculate onboarding period start and end dates
    
    Args:
        first_login_date (datetime): First login date
        period_days (int): Duration of onboarding period in days
    
    Returns:
        Tuple[datetime, datetime]: Start and end dates of onboarding period
    """
    start_date = first_login_date
    end_date = start_date + timedelta(days=period_days)
    return start_date, end_date

def write_dataframe_to_parquet(df: DataFrame, path: str, mode: str = "overwrite"):
    """
    Write DataFrame to parquet format
    
    Args:
        df (DataFrame): DataFrame to write
        path (str): Output path
        mode (str): Write mode (overwrite, append, etc.)
    """
    df.write.mode(mode).parquet(path)
    logging.info(f"DataFrame written to {path} with {df.count()} rows")

def read_parquet_file(spark: SparkSession, path: str) -> DataFrame:
    """
    Read parquet file into DataFrame
    
    Args:
        spark (SparkSession): Spark session
        path (str): File path
    
    Returns:
        DataFrame: Loaded DataFrame
    """
    df = spark.read.parquet(path)
    logging.info(f"DataFrame loaded from {path} with {df.count()} rows")
    return df

def show_sample_data(df: DataFrame, dataset_name: str, num_rows: int = 10):
    """
    Show sample data from DataFrame
    
    Args:
        df (DataFrame): Input DataFrame
        dataset_name (str): Name of the dataset
        num_rows (int): Number of rows to show
    """
    print(f"\n=== SAMPLE DATA: {dataset_name.upper()} ===")
    df.show(num_rows, truncate=False)
    print(f"Total rows: {df.count():,}")
    print("=" * 50)