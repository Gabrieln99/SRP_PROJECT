# extract/extract_mysql.py

from spark_session import get_spark_session

def extract_table(table_name):
    
    spark = get_spark_session("Flights_ETL_Extract_MySQL")
    
    jdbc_url = "jdbc:mysql://127.0.0.1:3306/flights_dw?useSSL=false"  # transactional model
    connection_properties = {
        "user": "root",
        "password": "root",
        "driver": "com.mysql.cj.jdbc.Driver"
    }
    
    print(f"Extracting table: {table_name}")
    df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=connection_properties)
    print(f"Extracted {df.count()} rows from {table_name}")
    
    return df

def extract_all_tables():
    """
    Ekstraktuje sve tabele iz transakcijskog modela letova
    """
    print("Starting extraction of all MySQL tables...")
    tables = {
        "airline": extract_table("airline"),
        "aircraft": extract_table("aircraft"),
        "route": extract_table("route"),
        "dep_delay": extract_table("dep_delay"),
        "arr_delay": extract_table("arr_delay"),
        "flight": extract_table("flight")
    }
    print("Completed extraction of all MySQL tables")
    
    return tables