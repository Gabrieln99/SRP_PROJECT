# transform/dimensions/dim_route.py
from pyspark.sql.functions import col, trim, initcap, lit, row_number, current_timestamp, coalesce
from pyspark.sql.window import Window
from spark_session import get_spark_session

def transform_route_dim(mysql_route_df, csv_flights_df=None):
    """
    Transform route data into the star schema dimension format.
    Combines data from MySQL transactional model and CSV (20% data).
    
    Args:
        mysql_route_df: DataFrame containing route data from MySQL
        csv_flights_df: DataFrame containing CSV flight data (20%)
        
    Returns:
        DataFrame with route dimension structure ready for loading
    """
    spark = get_spark_session()
    print("Transforming route dimension...")
    
    # --- Step 1: Normalize MySQL data ---
    mysql_df = (
        mysql_route_df
        .select(
            col("id").cast("long").alias("route_id"),
            trim(col("origin")).alias("origin"),
            trim(col("destination")).alias("destination"),
            initcap(trim(col("departure_city"))).alias("departure_city"),
            initcap(trim(col("departure_country"))).alias("departure_country"),
            initcap(trim(col("departure_airport_name"))).alias("departure_airport_name"),
            initcap(trim(col("destination_city"))).alias("destination_city"),
            initcap(trim(col("destination_country"))).alias("destination_country"),
            initcap(trim(col("destination_airport_name"))).alias("destination_airport_name"),
            col("distance").cast("double")
        )
        .dropDuplicates(["origin", "destination"])
    )
    
    # --- Step 2: Normalize CSV data ---
    if csv_flights_df:
        csv_df = (
            csv_flights_df
            .select(
                trim(col("origin")).alias("origin"),
                trim(col("destination")).alias("destination"),
                initcap(trim(col("departure_city"))).alias("departure_city"),
                initcap(trim(col("departure_country"))).alias("departure_country"),
                initcap(trim(col("departure_airport_name"))).alias("departure_airport_name"),
                initcap(trim(col("destination_city"))).alias("destination_city"),
                initcap(trim(col("destination_country"))).alias("destination_country"),
                initcap(trim(col("destination_airport_name"))).alias("destination_airport_name"),
                col("distance").cast("double")
            )
            .withColumn("route_id", lit(None).cast("long"))
            .dropDuplicates(["origin", "destination"])
        )
        
        # Combine MySQL and CSV data
        combined_df = mysql_df.unionByName(csv_df)
    else:
        combined_df = mysql_df
    
    # Remove duplicates after union
    combined_df = combined_df.dropDuplicates(["origin", "destination"])
    
    # --- Step 3: Add surrogate key ---
    window = Window.orderBy("origin", "destination")
    
    final_df = (
        combined_df
        .withColumn("route_tk", row_number().over(window))
        .select(
            "route_tk", "origin", "destination", "departure_city", "departure_country", 
            "departure_airport_name", "destination_city", "destination_country", 
            "destination_airport_name", "distance"
        )  # Removed SCD columns
    )
    
    print(f"Created route dimension with {final_df.count()} records")
    return final_df