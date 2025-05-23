# transform/dimensions/dim_aircraft.py
from pyspark.sql.functions import col, trim, lit, row_number, current_timestamp
from pyspark.sql.window import Window
from spark_session import get_spark_session

def transform_aircraft_dim(mysql_aircraft_df, csv_flights_df=None):
    """
    Transform aircraft data into the star schema dimension format.
    Combines data from MySQL transactional model and CSV (20% data).
    
    Args:
        mysql_aircraft_df: DataFrame containing aircraft data from MySQL
        csv_flights_df: DataFrame containing CSV flight data (20%)
        
    Returns:
        DataFrame with aircraft dimension structure ready for loading
    """
    spark = get_spark_session()
    print("Transforming aircraft dimension...")
    
    # --- Step 1: Normalize MySQL data ---
    mysql_df = (
        mysql_aircraft_df
        .select(
            col("id").cast("long").alias("aircraft_id"),
            trim(col("tailnum")).alias("tailnum")
        )
        .dropDuplicates(["tailnum"])
    )
    
    # --- Step 2: Normalize CSV data ---
    if csv_flights_df:
        csv_df = (
            csv_flights_df
            .select(
                trim(col("tailnum")).alias("tailnum")
            )
            .withColumn("aircraft_id", lit(None).cast("long"))
            .dropDuplicates(["tailnum"])
        )
        
        # Combine MySQL and CSV data
        combined_df = mysql_df.unionByName(csv_df)
    else:
        combined_df = mysql_df
    
    # Remove duplicates and null values
    combined_df = combined_df.dropDuplicates(["tailnum"])
    combined_df = combined_df.dropna(subset=["tailnum"])
    
    # --- Step 3: Add surrogate key ---
    window = Window.orderBy("tailnum")
    
    final_df = (
        combined_df
        .withColumn("aircraft_tk", row_number().over(window))
        .select("aircraft_tk", "tailnum")  # Removed SCD columns
    )
    
    print(f"Created aircraft dimension with {final_df.count()} records")
    return final_df