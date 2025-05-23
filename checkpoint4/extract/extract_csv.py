# extract/extract_csv.py
from spark_session import get_spark_session

def extract_from_csv(file_path):
    """
    Ekstraktuje podatke iz CSV datoteke koristeći Spark
    """
    spark = get_spark_session("ETL_Extract_CSV")
    
    print(f"Extracting data from CSV: {file_path}")
    
    df = spark.read \
        .option("header", True) \
        .option("inferSchema", True) \
        .csv(file_path)
    
    print(f"Extracted {df.count()} rows from CSV")
    
    return df