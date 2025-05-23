# transform/dimensions/dim_arr_delay.py
from pyspark.sql.functions import col, trim, lit, row_number, current_timestamp
from pyspark.sql.window import Window
from spark_session import get_spark_session

def transform_arr_delay_dim(mysql_arr_delay_df, csv_flights_df=None):
    """
    Transform arrival delay data into the star schema dimension format.
    Combines data from MySQL transactional model and CSV (20% data).
    
    Args:
        mysql_arr_delay_df: DataFrame containing arrival delay data from MySQL
        csv_flights_df: DataFrame containing CSV flight data (20%)
        
    Returns:
        DataFrame with arrival delay dimension structure ready for loading
    """
    spark = get_spark_session()
    print("Transforming arrival delay dimension...")
    
    # --- Step 1: Normalize MySQL data ---
    mysql_df = (
        mysql_arr_delay_df
        .select(
            col("id").cast("long").alias("arr_delay_id"),
            trim(col("reason_arr_delay")).alias("reason"),
            col("arr_delay_time").cast("double").alias("delay_time")
        )
        .dropDuplicates(["reason", "delay_time"])
    )
    
    # --- Step 2: Normalize CSV data ---
    if csv_flights_df:
        csv_df = (
            csv_flights_df
            .select(
                trim(col("reason_arr_delay")).alias("reason"),
                col("arr_delay_time").cast("double").alias("delay_time")
            )
            .withColumn("arr_delay_id", lit(None).cast("long"))
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
        .withColumn("arr_delay_tk", row_number().over(window))
        .select("arr_delay_tk", "reason", "delay_time")  # Removed SCD columns
    )
    
    print(f"Created arrival delay dimension with {final_df.count()} records")
    return final_df