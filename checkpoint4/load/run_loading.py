# load/run_loading.py

from pyspark.sql import DataFrame

def write_spark_df_to_mysql(spark_df: DataFrame, table_name: str, mode: str = "append"):  # Back to append
    """
    Upisuje Spark DataFrame u MySQL star schema bazu
    """
    jdbc_url = "jdbc:mysql://127.0.0.1:3306/dimenzijski_model?useSSL=false"  # star schema
    connection_properties = {
        "user": "root",
        "password": "root",
        "driver": "com.mysql.cj.jdbc.Driver"
    }

    print(f"Writing {spark_df.count()} rows to table `{table_name}` with mode `{mode}`...")
    print(f"Database URL: {jdbc_url}")
    
    try:
        spark_df.write.jdbc(
            url=jdbc_url,
            table=table_name,
            mode=mode,             
            properties=connection_properties
        )
        print(f"✅ Successfully wrote to `{table_name}`.")
    except Exception as e:
        print(f"❌ Error writing to `{table_name}`: {str(e)}")
        raise