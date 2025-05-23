# transform/dimensions/dim_dep_delay.py
from pyspark.sql.functions import col, trim, lit, row_number, current_timestamp
from pyspark.sql.window import Window
from spark_session import get_spark_session

def transform_dep_delay_dim(mysql_dep_delay_df, csv_flights_df=None):
    """
    Transform departure delay data into the star schema dimension format.
    Combines data from MySQL transactional model and CSV (20% data).
    
    Args:
        mysql_dep_delay_df: DataFrame containing departure delay data from MySQL
        csv_flights_df: DataFrame containing CSV flight data (20%)
        
    Returns:
        DataFrame with departure delay dimension structure ready for loading
    """
    spark = get_spark_session()
    print("Transforming departure delay dimension...")
    
    # --- Step 1: Normalize MySQL data ---
    mysql_df = (
        mysql_dep_delay_df
        .select(
            col("id").cast("long").alias("dep_delay_id"),
            trim(col("reason_dep_delay")).alias("reason"),
            col("dep_delay_time").cast("double").alias("delay_time")
        )
        .dropDuplicates(["reason", "delay_time"])
    )
    
    # --- Step 2: Normalize CSV data ---
    if csv_flights_df:
        csv_df = (
            csv_flights_df
            .select(
                trim(col("reason_dep_delay")).alias("reason"),
                col("dep_delay_time").cast("double").alias("delay_time")
            )
            .withColumn("dep_delay_id", lit(None).cast("long"))
            .dropDuplicates(["reason", "delay_time"])
        )
        
        # Combine MySQL and CSV data
        combined_df = mysql_df.unionByName(csv_df)
    else:
        combined_df = mysql_df
    
    # Remove duplicates after union
    combined_df = combined_df.dropDuplicates(["reason", "delay_time"])
    
    # --- Step 3: Add surrogate key ---
    window = Window.orderBy("reason", "delay_time")
    
    final_df = (
        combined_df
        .withColumn("dep_delay_tk", row_number().over(window))
        .select("dep_delay_tk", "reason", "delay_time")  # Removed SCD columns
    )
    
    print(f"Created departure delay dimension with {final_df.count()} records")
    return final_df