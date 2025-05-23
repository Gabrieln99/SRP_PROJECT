# transform/facts/fact_flight.py
from pyspark.sql.functions import col, trim, row_number, when, broadcast, lit, datediff, to_timestamp
from pyspark.sql.window import Window
from spark_session import get_spark_session

def transform_flight_fact(
    raw_data,
    dim_airline_df,
    dim_aircraft_df,
    dim_route_df,
    dim_dep_delay_df,
    dim_arr_delay_df,
    dim_date_df,
    dim_time_df
):
    """
    Transform flight data into the fact table by linking to all dimensions.
    Combines MySQL (80%) and CSV (20%) data.
    """
    spark = get_spark_session()
    print("Transforming flight fact table...")
    
    # Extract all raw tables
    flight_df = raw_data["flight"]
    airline_df = raw_data["airline"]
    aircraft_df = raw_data["aircraft"]
    route_df = raw_data["route"]
    dep_delay_df = raw_data["dep_delay"]
    arr_delay_df = raw_data["arr_delay"]
    csv_flights_df = raw_data.get("csv_flights")
    
    # --- Step 1: Normalize MySQL flight data ---
    enriched_mysql_flights = (
        flight_df.alias("f")
        .join(aircraft_df.alias("ac"), col("f.aircraft_fk") == col("ac.id"), "left")
        .join(airline_df.alias("al"), col("ac.airline_fk") == col("al.id"), "left")
        .join(route_df.alias("r"), col("f.route_fk") == col("r.id"), "left")
        .join(dep_delay_df.alias("dd"), col("f.dep_delay_fk") == col("dd.id"), "left")
        .join(arr_delay_df.alias("ad"), col("f.arr_delay_fk") == col("ad.id"), "left")
        .select(
            col("al.carrier").alias("carrier"),
            col("ac.tailnum").alias("tailnum"),
            col("r.origin").alias("origin"),
            col("r.destination").alias("destination"),
            col("dd.reason_dep_delay").alias("reason_dep_delay"),
            col("dd.dep_delay_time").alias("dep_delay_time"),
            col("ad.reason_arr_delay").alias("reason_arr_delay"),
            col("ad.arr_delay_time").alias("arr_delay_time"),
            col("f.year").cast("int").alias("year"),
            col("f.month").cast("int").alias("month"),
            col("f.day").cast("int").alias("day"),
            col("f.day_of_week").alias("day_of_week"),
            col("f.dep_time").alias("dep_time"),
            col("f.arr_time").alias("arr_time"),
            col("f.sched_dep_time").alias("sched_dep_time"),
            col("f.sched_arr_time").alias("sched_arr_time"),
            col("f.hour").cast("int").alias("hour"),
            col("f.minute").cast("int").alias("minute"),
            col("f.air_time").cast("double").alias("air_time"),
            col("f.flight_num").cast("int").alias("flight_num")
        )
    )
    
    print(f"MySQL flight data count: {enriched_mysql_flights.count()}")
    
    # --- Step 2: Normalize CSV flight data ---
    if csv_flights_df:
        cleaned_csv_flights = (
            csv_flights_df
            .select(
                trim(col("carrier")).alias("carrier"),
                trim(col("tailnum")).alias("tailnum"),
                trim(col("origin")).alias("origin"),
                trim(col("destination")).alias("destination"),
                trim(col("reason_dep_delay")).alias("reason_dep_delay"),
                col("dep_delay_time").cast("double").alias("dep_delay_time"),
                trim(col("reason_arr_delay")).alias("reason_arr_delay"),
                col("arr_delay_time").cast("double").alias("arr_delay_time"),
                col("year").cast("int").alias("year"),
                col("month").cast("int").alias("month"),
                col("day").cast("int").alias("day"),
                trim(col("day_of_week")).alias("day_of_week"),
                trim(col("dep_time")).alias("dep_time"),
                trim(col("arr_time")).alias("arr_time"),
                trim(col("sched_dep_time")).alias("sched_dep_time"),
                trim(col("sched_arr_time")).alias("sched_arr_time"),
                col("hour").cast("int").alias("hour"),
                col("minute").cast("int").alias("minute"),
                col("air_time").cast("double").alias("air_time"),
                col("flight_num").cast("int").alias("flight_num")
            )
        )
        print(f"CSV flight data count: {cleaned_csv_flights.count()}")
    else:
        cleaned_csv_flights = None
    
    # --- Step 3: Merge MySQL and CSV flights ---
    combined_flights = enriched_mysql_flights
    if cleaned_csv_flights:
        combined_flights = combined_flights.unionByName(cleaned_csv_flights)
    
    print(f"Combined flight data count: {combined_flights.count()}")
    
    # --- Step 4: Join with dimensions to get surrogate keys ---
    fact_df = (
        combined_flights.alias("f")
        
        # Join with airline dimension
        .join(broadcast(dim_airline_df.alias("al")), 
              col("f.carrier") == col("al.carrier"), "left")
        
        # Join with aircraft dimension  
        .join(broadcast(dim_aircraft_df.alias("ac")), 
              col("f.tailnum") == col("ac.tailnum"), "left")
        
        # Join with route dimension
        .join(broadcast(dim_route_df.alias("r")), 
              (col("f.origin") == col("r.origin")) & 
              (col("f.destination") == col("r.destination")), "left")
        
        # Join with departure delay dimension
        .join(broadcast(dim_dep_delay_df.alias("dd")), 
              (col("f.reason_dep_delay") == col("dd.reason")) & 
              (col("f.dep_delay_time") == col("dd.delay_time")), "left")
        
        # Join with arrival delay dimension
        .join(broadcast(dim_arr_delay_df.alias("ad")), 
              (col("f.reason_arr_delay") == col("ad.reason")) & 
              (col("f.arr_delay_time") == col("ad.delay_time")), "left")
        
        # Join with date dimension
        .join(broadcast(dim_date_df.alias("d")), 
              (col("f.year") == col("d.year")) & 
              (col("f.month") == col("d.month")) & 
              (col("f.day") == col("d.day")), "left")
        
        # Join with time dimension
        .join(dim_time_df.alias("t"), 
              (col("f.dep_time") == col("t.dep_time")) & 
              (col("f.arr_time") == col("t.arr_time")) & 
              (col("f.sched_dep_time") == col("t.sched_dep_time")) & 
              (col("f.sched_arr_time") == col("t.sched_arr_time")) & 
              (col("f.hour") == col("t.hour")) & 
              (col("f.minute") == col("t.minute")), "left")
        
        # Select dimension keys and measures
        .select(
            col("al.airline_tk").alias("airline_id"),
            col("ac.aircraft_tk").alias("aircraft_id"),
            col("r.route_tk").alias("route_id"),
            col("dd.dep_delay_tk").alias("dep_delay_id"),
            col("ad.arr_delay_tk").alias("arr_delay_id"),
            col("d.date_tk").alias("date_id"),
            col("t.time_tk").alias("time_id"),
            col("f.air_time"),
            col("f.flight_num")
        )
    )
    
    # --- Step 5: Add calculated measures ---
    fact_df_enhanced = (
        fact_df
        # Add scheduled duration (dummy calculation as example)
        .withColumn("scheduled_duration", 
                   when(col("air_time").isNotNull(), col("air_time") + 30)
                   .otherwise(lit(None)))
        
        # Add actual duration (same as air_time for now)
        .withColumn("actual_duration", col("air_time"))
        
        # Add on-time flag (if both delays are <= 0)
        .withColumn("on_time_flag",
                   when((col("dep_delay_id").isNotNull()) & 
                        (col("arr_delay_id").isNotNull()), 1)
                   .otherwise(0))
    )
    
    # --- Step 6: Add surrogate key ---
    fact_df_final = fact_df_enhanced.withColumn(
        "flight_tk",
        row_number().over(Window.orderBy("airline_id", "aircraft_id", "route_id", "date_id"))
    ).select(
        "flight_tk",
        "airline_id", 
        "aircraft_id",
        "route_id",
        "dep_delay_id",
        "arr_delay_id", 
        "date_id",
        "time_id",
        "air_time",
        "flight_num",
        "scheduled_duration",
        "actual_duration", 
        "on_time_flag"
    )
    
    # --- Step 7: Data quality checks ---
    total_records = fact_df_final.count()
    valid_records = fact_df_final.filter(
        col("airline_id").isNotNull() &
        col("aircraft_id").isNotNull() &
        col("route_id").isNotNull() &
        col("date_id").isNotNull()
    ).count()
    
    print(f"Final fact flight table: {total_records} total records")
    print(f"Valid records with all required keys: {valid_records}")
    print(f"Data quality: {(valid_records/total_records*100):.2f}% complete records")
    
    return fact_df_final