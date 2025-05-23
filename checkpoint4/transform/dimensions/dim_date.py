# transform/dimensions/dim_date.py
from pyspark.sql.functions import col, trim, lit, row_number, current_timestamp, when
from pyspark.sql.window import Window
from spark_session import get_spark_session

def transform_date_dim(mysql_flight_df, csv_flights_df=None):
    """
    Transform date data into the star schema dimension format.
    Creates a comprehensive date dimension with useful attributes.
    
    Args:
        mysql_flight_df: DataFrame containing flight data from MySQL
        csv_flights_df: DataFrame containing CSV flight data (20%)
        
    Returns:
        DataFrame with date dimension structure ready for loading
    """
    spark = get_spark_session()
    print("Transforming date dimension...")
    
    # --- Step 1: Extract dates from MySQL data ---
    mysql_df = (
        mysql_flight_df
        .select(
            col("year").cast("int"),
            col("month").cast("int"),
            col("day").cast("int"),
            trim(col("day_of_week")).alias("day_of_week")
        )
        .dropDuplicates(["year", "month", "day"])
    )
    
    # --- Step 2: Extract dates from CSV data ---
    if csv_flights_df:
        csv_df = (
            csv_flights_df
            .select(
                col("year").cast("int"),
                col("month").cast("int"),
                col("day").cast("int"),
                trim(col("day_of_week")).alias("day_of_week")
            )
            .dropDuplicates(["year", "month", "day"])
        )
        
        # Combine MySQL and CSV data
        combined_df = mysql_df.unionByName(csv_df)
    else:
        combined_df = mysql_df
    
    # Remove duplicates after union
    combined_df = combined_df.dropDuplicates(["year", "month", "day"])
    
    # --- Step 3: Add calculated date attributes ---
    enhanced_df = (
        combined_df
        # Calculate quarter
        .withColumn("quarter", 
                   when(col("month").between(1, 3), 1)
                   .when(col("month").between(4, 6), 2)
                   .when(col("month").between(7, 9), 3)
                   .otherwise(4))
        
        # Add month names
        .withColumn("month_name",
                   when(col("month") == 1, "January")
                   .when(col("month") == 2, "February")
                   .when(col("month") == 3, "March")
                   .when(col("month") == 4, "April")
                   .when(col("month") == 5, "May")
                   .when(col("month") == 6, "June")
                   .when(col("month") == 7, "July")
                   .when(col("month") == 8, "August")
                   .when(col("month") == 9, "September")
                   .when(col("month") == 10, "October")
                   .when(col("month") == 11, "November")
                   .otherwise("December"))
        
        # Add weekend flag
        .withColumn("is_weekend",
                   when(col("day_of_week").isin(["Saturday", "Sunday"]), 1)
                   .otherwise(0))
    )
    
    # --- Step 4: Add surrogate key ---
    window = Window.orderBy("year", "month", "day")
    
    final_df = (
        enhanced_df
        .withColumn("date_tk", row_number().over(window))
        .select(
            "date_tk", "year", "month", "day", "day_of_week", 
            "quarter", "month_name", "is_weekend"
        )  # Removed SCD columns
    )
    
    print(f"Created date dimension with {final_df.count()} records")
    return final_df