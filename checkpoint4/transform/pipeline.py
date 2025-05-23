# transform/pipeline.py

from transform.dimensions.dim_airline import transform_airline_dim
from transform.dimensions.dim_aircraft import transform_aircraft_dim
from transform.dimensions.dim_route import transform_route_dim
from transform.dimensions.dim_dep_delay import transform_dep_delay_dim
from transform.dimensions.dim_arr_delay import transform_arr_delay_dim
from transform.dimensions.dim_date import transform_date_dim
from transform.dimensions.dim_time import transform_time_dim
from transform.facts.fact_flight import transform_flight_fact

def run_transformations(raw_data):
    """
    Glavni transformation pipeline koji procesira sve dimenzije i fact tabele
    """
    
    # Transform dimensions
    airline_dim = transform_airline_dim(
        raw_data["airline"],
        csv_flights_df=raw_data.get("csv_flights")
    )
    print("1️⃣ Airline dimension complete")
    
    aircraft_dim = transform_aircraft_dim(
        raw_data["aircraft"],
        csv_flights_df=raw_data.get("csv_flights")
    )
    print("2️⃣ Aircraft dimension complete")
    
    route_dim = transform_route_dim(
        raw_data["route"],
        csv_flights_df=raw_data.get("csv_flights")
    )
    print("3️⃣ Route dimension complete")
    
    dep_delay_dim = transform_dep_delay_dim(
        raw_data["dep_delay"],
        csv_flights_df=raw_data.get("csv_flights")
    )
    print("4️⃣ Departure delay dimension complete")
    
    arr_delay_dim = transform_arr_delay_dim(
        raw_data["arr_delay"],
        csv_flights_df=raw_data.get("csv_flights")
    )
    print("5️⃣ Arrival delay dimension complete")
    
    date_dim = transform_date_dim(
        raw_data["flight"],
        csv_flights_df=raw_data.get("csv_flights")
    )
    print("6️⃣ Date dimension complete")
    
    time_dim = transform_time_dim(
        raw_data["flight"],
        csv_flights_df=raw_data.get("csv_flights")
    )
    print("7️⃣ Time dimension complete")
    
    # Transform fact table - mora biti poslednje jer koristi sve dimenzije
    fact_flight = transform_flight_fact(
        raw_data,
        airline_dim,
        aircraft_dim,
        route_dim,
        dep_delay_dim,
        arr_delay_dim,
        date_dim,
        time_dim
    )
    print("8️⃣ Flight fact table complete")
    
    return {
        "dim_airline": airline_dim,
        "dim_aircraft": aircraft_dim,
        "dim_route": route_dim,
        "dim_dep_delay": dep_delay_dim,
        "dim_arr_delay": arr_delay_dim,
        "dim_date": date_dim,
        "dim_time": time_dim,
        "fact_flight": fact_flight
    }