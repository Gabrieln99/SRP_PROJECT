# transform/dimensions/dim_time.py
from pyspark.sql.functions import col, trim, lit, row_number, current_timestamp, when
from pyspark.sql.window import Window
from spark_session import get_spark_session

def transform_time_dim(mysql_flight_df, csv_flights_df=None):
    """
    Transform time data into the star schema dimension format.
    Creates a comprehensive time dimension with useful attributes.
    
    Args:
        mysql_flight_df: DataFrame containing flight data from MySQL
        csv_flights_df: DataFrame containing CSV flight data (20%)
        
    Returns:
        DataFrame with time dimension structure ready for loading
    """
    spark = get_spark_session()
    print("Transforming time dimension...")
    
    # --- Step 1: Extract times from MySQL data ---
    mysql_df = (
        mysql_flight_df
        .select(
            trim(col("dep_time")).alias("dep_time"),
            trim(col("arr_time")).alias("arr_time"),
            trim(col("sched_dep_time")).alias("sched_dep_time"),
            trim(col("sched_arr_time")).alias("sched_arr_time"),
            col("hour").cast("int"),
            col("minute").cast("int")
        )
        .dropDuplicates(["dep_time", "arr_time", "sched_dep_time", "sched_arr_time"])
    )
    
    # --- Step 2: Extract times from CSV data ---
    if csv_flights_df:
        csv_df = (
            csv_flights_df
            .select(
                trim(col("dep_time")).alias("dep_time"),
                trim(col("arr_time")).alias("arr_time"),
                trim(col("sched_dep_time")).alias("sched_dep_time"),
                trim(col("sched_arr_time")).alias("sched_arr_time"),
                col("hour").cast("int"),
                col("minute").cast("int")
            )
            .dropDuplicates(["dep_time", "arr_time", "sched_dep_time", "sched_arr_time"])
        )
        
        # Combine MySQL and CSV data
        combined_df = mysql_df.unionByName(csv_df)
    else:
        combined_df = mysql_df
    
    # Remove duplicates after union
    combined_df = combined_df.dropDuplicates(["dep_time", "arr_time", "sched_dep_time", "sched_arr_time"])
    
    # --- Step 3: Add calculated time attributes ---
    enhanced_df = (
        combined_df
        # Truncate time fields to fit MySQL column size (usually VARCHAR(10))
        .withColumn("dep_time", col("dep_time").substr(1, 10))
        .withColumn("arr_time", col("arr_time").substr(1, 10))  
        .withColumn("sched_dep_time", col("sched_dep_time").substr(1, 10))
        .withColumn("sched_arr_time", col("sched_arr_time").substr(1, 10))
        
        # Add time of day based on scheduled departure hour
        .withColumn("time_of_day",
                   when(col("hour").between(5, 11), "Morning")
                   .when(col("hour").between(12, 17), "Afternoon")
                   .when(col("hour").between(18, 21), "Evening")
                   .otherwise("Night"))
        
        # Add peak hour flag (rush hours: 6-9 AM and 5-8 PM)
        .withColumn("is_peak_hour",
                   when(col("hour").between(6, 9) | col("hour").between(17, 20), 1)
                   .otherwise(0))
    )
    
    # --- Step 4: Add surrogate key ---
    window = Window.orderBy("hour", "minute", "dep_time")
    
    final_df = (
        enhanced_df
        .withColumn("time_tk", row_number().over(window))
        .select(
            "time_tk", "dep_time", "arr_time", "sched_dep_time", "sched_arr_time",
            "hour", "minute", "time_of_day", "is_peak_hour"
        )  # Removed SCD columns
    )
    
    print(f"Created time dimension with {final_df.count()} records")
    return final_df