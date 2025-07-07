"""
Spark configuration for the ETL pipeline with AstraDB support
"""
import os
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def create_spark_session(app_name="Fintech_ETL", master="local[*]", use_astradb=True):
    """
    Create and configure Spark session for the ETL pipeline with AstraDB support
    
    Args:
        app_name (str): Name of the Spark application
        master (str): Spark master URL
        use_astradb (bool): Whether to configure for AstraDB or local Cassandra
    
    Returns:
        SparkSession: Configured Spark session
    """
    conf = SparkConf()
    conf.setAppName(app_name)
    conf.setMaster(master)
    
    # Spark SQL configurations
    conf.set("spark.sql.adaptive.enabled", "true")
    conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
    
    # Memory configurations
    conf.set("spark.executor.memory", "4g")
    conf.set("spark.driver.memory", "2g")
    
    # Cassandra connector configurations (compatible with PySpark 4.0.0 / Scala 2.13)
    conf.set("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.5.1")
    conf.set("spark.jars.repositories", "https://repo1.maven.org/maven2")
    
    if use_astradb:
        # AstraDB Configuration
        secure_bundle_path = os.getenv("ASTRA_DB_SECURE_BUNDLE_PATH", "astradb/secure-connect-big-data.zip")
        astra_token = os.getenv("ASTRA_DB_TOKEN")
        astra_keyspace = os.getenv("ASTRA_DB_KEYSPACE", "fintech_analytics")
        
        if not astra_token:
            raise ValueError("ASTRA_DB_TOKEN environment variable is required for AstraDB connection")
        
        # Convert to absolute path for Spark
        if not os.path.isabs(secure_bundle_path):
            secure_bundle_path = os.path.abspath(secure_bundle_path)
        
        if not os.path.exists(secure_bundle_path):
            raise FileNotFoundError(f"Secure connect bundle not found: {secure_bundle_path}")
        
        # Distribute the secure bundle file to all Spark executors
        conf.set("spark.files", "/home/benjamin.delasoie/itba/big_data/astradb/secure-connect-big-data.zip")
        
        # Configure for AstraDB using secure connect bundle (updated for connector 3.5.1)
        # Use just the filename since it will be distributed via spark.files
        bundle_filename = os.path.basename(secure_bundle_path)
        conf.set("spark.cassandra.connection.config.cloud.path", "secure-connect-big-data.zip")
        conf.set("spark.cassandra.auth.username", "token")
        conf.set("spark.cassandra.auth.password", astra_token)
        
        # Connection settings optimized for AstraDB cloud
        conf.set("spark.cassandra.connection.timeoutMS", "60000")
        conf.set("spark.cassandra.read.timeoutMS", "60000")
        conf.set("spark.cassandra.connection.keepAliveMS", "60000")
        
        # Disable connection pooling that can cause issues with cloud
        conf.set("spark.cassandra.connection.reconnectionDelayMS.max", "60000")
        conf.set("spark.cassandra.connection.reconnectionDelayMS.min", "1000")
        
    else:
        # Local Cassandra Configuration (fallback)
        conf.set("spark.cassandra.connection.host", "localhost")
        conf.set("spark.cassandra.connection.port", "9042")
    
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    
    # Set log level to reduce verbosity
    spark.sparkContext.setLogLevel("WARN")
    
    return spark

def get_cassandra_config(use_astradb=True):
    """
    Get Cassandra/AstraDB connection configuration
    
    Args:
        use_astradb (bool): Whether to return AstraDB or local Cassandra config
    
    Returns:
        dict: Cassandra connection parameters
    """
    if use_astradb:
        return {
            "keyspace": os.getenv("ASTRA_DB_KEYSPACE", "fintech_analytics"),
            "secure_bundle_path": os.getenv("ASTRA_DB_SECURE_BUNDLE_PATH", "astradb/secure-connect-big-data.zip"),
            "token": os.getenv("ASTRA_DB_TOKEN"),
            "datacenter": os.getenv("ASTRA_DB_DATACENTER", "eu-west-1")
        }
    else:
        return {
            "spark.cassandra.connection.host": "localhost",
            "spark.cassandra.connection.port": "9042",
            "keyspace": "fintech_analytics"
        }

def get_astradb_connection_info():
    """
    Get AstraDB connection information for direct API access
    
    Returns:
        dict: AstraDB connection parameters for astrapy
    """
    return {
        "token": os.getenv("ASTRA_DB_TOKEN"),
        "api_endpoint": f"https://{os.getenv('ASTRA_DB_ID')}-{os.getenv('ASTRA_DB_REGION')}.apps.astra.datastax.com",
        "keyspace": os.getenv("ASTRA_DB_KEYSPACE", "fintech_analytics"),
        "database_id": os.getenv("ASTRA_DB_ID"),
        "region": os.getenv("ASTRA_DB_REGION")
    }