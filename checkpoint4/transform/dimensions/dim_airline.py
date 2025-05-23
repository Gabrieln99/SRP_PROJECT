# transform/dimensions/dim_airline.py
from pyspark.sql.functions import col, trim, initcap, lit, row_number, current_timestamp, coalesce
from pyspark.sql.window import Window
from spark_session import get_spark_session

def transform_airline_dim(mysql_airline_df, csv_flights_df=None):
    """
    Transform airline data into the star schema dimension format.
    Combines data from MySQL transactional model and CSV (20% data).
    
    Args:
        mysql_airline_df: DataFrame containing airline data from MySQL
        csv_flights_df: DataFrame containing CSV flight data (20%)
        
    Returns:
        DataFrame with airline dimension structure ready for loading
    """
    spark = get_spark_session()
    print("Transforming airline dimension...")
    
    # --- Step 1: Normalize MySQL data ---
    mysql_df = (
        mysql_airline_df
        .select(
            col("id").cast("long").alias("airline_id"),
            trim(col("carrier")).alias("carrier"),
            initcap(trim(col("airline_name"))).alias("airline_name")
        )
        .dropDuplicates(["carrier"])
    )
    
    # --- Step 2: Normalize CSV data ---
    if csv_flights_df:
        csv_df = (
            csv_flights_df
            .select(
                trim(col("carrier")).alias("carrier"),
                initcap(trim(col("airline_name"))).alias("airline_name")
            )
            .withColumn("airline_id", lit(None).cast("long"))
            .dropDuplicates(["carrier"])
        )
        
        # Combine MySQL and CSV data
        combined_df = mysql_df.unionByName(csv_df)
    else:
        combined_df = mysql_df
    
    # Remove duplicates after union
    combined_df = combined_df.dropDuplicates(["carrier"])
    
    # --- Step 3: Add surrogate key ---
    window = Window.orderBy("carrier")
    
    final_df = (
        combined_df
        .withColumn("airline_tk", row_number().over(window))
        .select("airline_tk", "carrier", "airline_name")  # Removed SCD columns
    )
    
    print(f"Created airline dimension with {final_df.count()} records")
    return final_df