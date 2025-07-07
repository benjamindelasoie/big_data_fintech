"""
ETL pipeline configuration and constants
"""
import os
from datetime import datetime, timedelta

# Data paths
DATA_DIR = "data"
RAW_DATA_PATHS = {
    "users": os.path.join(DATA_DIR, "dim_users.csv"),
    "transactions": os.path.join(DATA_DIR, "bt_users_transactions.csv"),
    "onboarding": os.path.join(DATA_DIR, "lk_onboarding.csv")
}

# Processed data paths
PROCESSED_DATA_DIR = "processed_data"
PROCESSED_DATA_PATHS = {
    "clean_users": os.path.join(PROCESSED_DATA_DIR, "clean_users.parquet"),
    "clean_transactions": os.path.join(PROCESSED_DATA_DIR, "clean_transactions.parquet"),
    "clean_onboarding": os.path.join(PROCESSED_DATA_DIR, "clean_onboarding.parquet"),
    "user_metrics": os.path.join(PROCESSED_DATA_DIR, "user_metrics.parquet"),
    "final_analytics": os.path.join(PROCESSED_DATA_DIR, "final_analytics.parquet")
}

# Business logic constants
TRANSACTION_TYPES = {
    "PAYMENT": list(range(1, 8)),  # 1-7 are payment transactions
    "COLLECTION": [8, 9]           # 8-9 are collection transactions
}

USER_SEGMENTS = {
    "INDIVIDUALS": 1,
    "SELLERS": 2,
    "UNKNOWN": 0  # Default for users without transactions
}

# Onboarding period in days
ONBOARDING_PERIOD_DAYS = 30

# A/B Testing configuration
AB_TEST_CONFIG = {
    "CONTROL_GROUP_PERCENTAGE": 5,
    "TREATMENT_GROUP_PERCENTAGE": 95,
    "COUNTRY_FILTER": "Brazil",  # Users with MLB prefix
    "USER_ID_PREFIX": "MLB"
}

# Metrics calculation rules
HABIT_RULES = {
    "INDIVIDUALS": {
        "min_transactions": 5,
        "min_different_days": 5,
        "transaction_types": TRANSACTION_TYPES["PAYMENT"] + TRANSACTION_TYPES["COLLECTION"]
    },
    "SELLERS": {
        "min_transactions": 5,
        "min_different_days": 1,  # Any days for sellers
        "transaction_types": TRANSACTION_TYPES["COLLECTION"]
    },
    "UNKNOWN": {  # Default to individuals rule
        "min_transactions": 5,
        "min_different_days": 5,
        "transaction_types": TRANSACTION_TYPES["PAYMENT"] + TRANSACTION_TYPES["COLLECTION"]
    }
}

# Cassandra configuration
CASSANDRA_CONFIG = {
    "keyspace": "fintech_analytics",
    "replication_factor": 1,
    "tables": {
        "user_metrics": "user_metrics",
        "transaction_summary": "transaction_summary",
        "ab_test_results": "ab_test_results",
        "funnel_analysis": "funnel_analysis"
    }
}

# Data quality thresholds
DATA_QUALITY_THRESHOLDS = {
    "max_missing_percentage": 10,  # Maximum 10% missing values allowed
    "min_date": "2022-01-01",
    "max_date": "2022-12-31"
}

# Logging configuration
LOG_CONFIG = {
    "log_level": "INFO",
    "log_format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
}