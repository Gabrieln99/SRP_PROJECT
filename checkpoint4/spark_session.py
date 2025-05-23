import os
import logging
import shutil
import urllib.request
from pathlib import Path
from pyspark.sql import SparkSession

def get_spark_session(app_name="ETL_App"):
    """
    Create and return a Spark session with MySQL connector configured.
    """
    # Get the directory where the script is located
    current_dir = Path(__file__).parent.absolute()
    
    # Set up Hadoop home for Windows to avoid warnings
    hadoop_home = os.path.join(current_dir, "hadoop")
    hadoop_bin = os.path.join(hadoop_home, "bin")
    
    # Create Hadoop directory structure if it doesn't exist
    if not os.path.exists(hadoop_bin):
        os.makedirs(hadoop_bin, exist_ok=True)
        logging.warning(f"Created Hadoop bin directory at: {hadoop_bin}")
        
        # Download winutils.exe for Windows
        winutils_url = "https://github.com/cdarlint/winutils/raw/master/hadoop-3.2.2/bin/winutils.exe"
        winutils_path = os.path.join(hadoop_bin, "winutils.exe")
        
        try:
            logging.warning(f"Downloading winutils.exe from {winutils_url}")
            urllib.request.urlretrieve(winutils_url, winutils_path)
            logging.warning("winutils.exe downloaded successfully")
        except Exception as e:
            logging.error(f"Failed to download winutils.exe: {e}")
            
    # Set environment variables
    os.environ["HADOOP_HOME"] = str(hadoop_home)
    os.environ["PATH"] = hadoop_bin + os.pathsep + os.environ["PATH"]
    os.environ["JAVA_HOME"] = os.getenv("JAVA_HOME", "")  # Ensure JAVA_HOME is set
    
    # Define connector path relative to the script location
    connector_dir = os.path.join(current_dir, "connectors")
    connector_path = os.path.join(connector_dir, "mysql-connector-j-9.2.0.jar")
    
    # Create connectors directory if it doesn't exist
    if not os.path.exists(connector_dir):
        os.makedirs(connector_dir)
        logging.warning(f"Created connectors directory at: {connector_dir}")
    
    # Check if connector exists
    if not os.path.exists(connector_path):
        # Try alternate filenames
        alternate_names = [
            "mysql-connector-java-8.0.28.jar",
            "mysql-connector-java-8.0.30.jar",
            "mysql-connector-j-8.3.0.jar"
        ]
        
        for name in alternate_names:
            alt_path = os.path.join(connector_dir, name)
            if os.path.exists(alt_path):
                connector_path = alt_path
                break
        else:  # No connector found
            error_msg = f"MySQL connector JAR not found at: {connector_path}"
            logging.error(error_msg)
            raise FileNotFoundError(error_msg)
    
    # Build the Spark session with additional configurations for Windows
    spark = SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .config("spark.jars", connector_path) \
        .config("spark.driver.extraClassPath", connector_path) \
        .config("spark.executor.extraClassPath", connector_path) \
        .config("spark.ui.showConsoleProgress", "false") \
        .config("spark.hadoop.fs.permissions.umask-mode", "022") \
        .config("spark.hadoop.dfs.permissions", "false") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
        .config("spark.sql.catalogImplementation", "in-memory") \
        .config("spark.sql.warehouse.dir", os.path.join(current_dir, "spark-warehouse")) \
        .config("spark.driver.host", "localhost") \
        .getOrCreate()
    
    # Set log level to ERROR to prevent warnings
    spark.sparkContext.setLogLevel("ERROR")
    
    return spark