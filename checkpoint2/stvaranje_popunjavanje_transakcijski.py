#!/usr/bin/env python3
import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, Float, ForeignKey, Date, MetaData
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.exc import IntegrityError
from datetime import datetime
import numpy as np

# Configuration - modify these as needed
CSV_FILE_PATH = "flights_najbolji_PROCESSED.csv"  # 80% dataset
DB_CONNECTION = "mysql+pymysql://root:root@localhost/flights_transactional_db"
DROP_EXISTING = True  # Set to False if you want to keep existing tables

# Load the CSV data
print(f"Loading data from {CSV_FILE_PATH}...")
df = pd.read_csv(CSV_FILE_PATH, delimiter=',')
print(f"CSV size: {df.shape}")
print(df.head())

# Create SQLAlchemy base
Base = declarative_base()

# Define the schema
class Airlines(Base):
    __tablename__ = 'airlines'
    
    carrier = Column(String(3), primary_key=True)
    airline_name = Column(String(100), nullable=False)
    
    # Relationships
    aircraft = relationship("Aircraft", back_populates="airline")
    flights = relationship("Flights", back_populates="airline")
    
    def __repr__(self):
        return f"<Airline(carrier='{self.carrier}', name='{self.airline_name}')>"

class Airports(Base):
    __tablename__ = 'airports'
    
    airport_code = Column(String(5), primary_key=True)
    airport_name = Column(String(200))
    city = Column(String(100))
    country = Column(String(100))
    
    # Relationships
    origin_flights = relationship("Flights", foreign_keys="Flights.origin", back_populates="origin_airport")
    destination_flights = relationship("Flights", foreign_keys="Flights.destination", back_populates="destination_airport")
    
    def __repr__(self):
        return f"<Airport(code='{self.airport_code}', name='{self.airport_name}')>"

class Aircraft(Base):
    __tablename__ = 'aircraft'
    
    tailnum = Column(String(10), primary_key=True)
    carrier = Column(String(3), ForeignKey('airlines.carrier'), nullable=False)
    
    # Relationships
    airline = relationship("Airlines", back_populates="aircraft")
    flights = relationship("Flights", back_populates="aircraft")
    
    def __repr__(self):
        return f"<Aircraft(tailnum='{self.tailnum}', carrier='{self.carrier}')>"

class DateDimension(Base):
    __tablename__ = 'date_dimension'
    
    date_id = Column(Integer, primary_key=True, autoincrement=True)
    year = Column(Integer, nullable=False)
    month = Column(Integer, nullable=False)
    day = Column(Integer, nullable=False)
    day_of_week = Column(String(10), nullable=False)
    full_date = Column(Date, nullable=False, unique=True)
    
    # Relationships
    flights = relationship("Flights", back_populates="date")
    
    def __repr__(self):
        return f"<DateDimension(date_id={self.date_id}, full_date='{self.full_date}')>"

class Flights(Base):
    __tablename__ = 'flights'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    flight_num = Column(Integer, nullable=False)
    carrier = Column(String(3), ForeignKey('airlines.carrier'), nullable=False)
    tailnum = Column(String(10), ForeignKey('aircraft.tailnum'), nullable=False)
    origin = Column(String(5), ForeignKey('airports.airport_code'), nullable=False)
    destination = Column(String(5), ForeignKey('airports.airport_code'), nullable=False)
    date_id = Column(Integer, ForeignKey('date_dimension.date_id'), nullable=False)
    
    dep_time = Column(String(5))
    sched_dep_time = Column(String(5), nullable=False)
    arr_time = Column(String(5))
    sched_arr_time = Column(String(5), nullable=False)
    
    air_time = Column(Float)
    distance = Column(Integer)
    hour = Column(Integer)
    minute = Column(Integer)
    
    # Relationships
    airline = relationship("Airlines", back_populates="flights")
    aircraft = relationship("Aircraft", back_populates="flights")
    origin_airport = relationship("Airports", foreign_keys=[origin], back_populates="origin_flights")
    destination_airport = relationship("Airports", foreign_keys=[destination], back_populates="destination_flights")
    date = relationship("DateDimension", back_populates="flights")
    delays = relationship("Delays", back_populates="flight")
    
    def __repr__(self):
        return f"<Flight(id={self.id}, carrier='{self.carrier}', flight_num={self.flight_num})>"

class Delays(Base):
    __tablename__ = 'delays'
    
    delay_id = Column(Integer, primary_key=True, autoincrement=True)
    flight_id = Column(Integer, ForeignKey('flights.id'), nullable=False)
    dep_delay_time = Column(Float)
    reason_dep_delay = Column(String(100))
    arr_delay_time = Column(Float)
    reason_arr_delay = Column(String(100))
    
    # Relationships
    flight = relationship("Flights", back_populates="delays")
    
    def __repr__(self):
        return f"<Delay(delay_id={self.delay_id}, flight_id={self.flight_id})>"

# Create the database engine
engine = create_engine(DB_CONNECTION, echo=False)

# Drop and recreate tables if specified
if DROP_EXISTING:
    print("Dropping existing tables...")
    Base.metadata.drop_all(engine)

# Create tables
print("Creating database tables...")
Base.metadata.create_all(engine)

# Create session
Session = sessionmaker(bind=engine)
session = Session()

def populate_database():
    try:
        # 1. Insert Airlines
        print("Inserting Airlines...")
        airlines = df[['carrier', 'airline_name']].drop_duplicates()
        
        for _, airline in airlines.iterrows():
            session.add(Airlines(
                carrier=airline['carrier'],
                airline_name=airline['airline_name']
            ))
        
        session.commit()
        print(f"Successfully inserted {len(airlines)} airlines")
        
        # 2. Insert Airports
        print("Inserting Airports...")
        # Extract origin airports
        origin_airports = df[['origin', 'departure_airport_name', 'departure_city', 'departure_country']].drop_duplicates()
        origin_airports.columns = ['airport_code', 'airport_name', 'city', 'country']
        
        # Extract destination airports
        dest_airports = df[['destination', 'destination_airport_name', 'destination_city', 'destination_country']].drop_duplicates()
        dest_airports.columns = ['airport_code', 'airport_name', 'city', 'country']
        
        # Combine and deduplicate
        all_airports = pd.concat([origin_airports, dest_airports]).drop_duplicates(subset=['airport_code'])
        
        for _, airport in all_airports.iterrows():
            session.add(Airports(
                airport_code=airport['airport_code'],
                airport_name=airport['airport_name'] if not pd.isna(airport['airport_name']) else None,
                city=airport['city'] if not pd.isna(airport['city']) else None,
                country=airport['country'] if not pd.isna(airport['country']) else None
            ))
        
        session.commit()
        print(f"Successfully inserted {len(all_airports)} airports")
        
        # 3. Insert Aircraft
        print("Inserting Aircraft...")
        aircraft = df[['tailnum', 'carrier']].drop_duplicates().dropna(subset=['tailnum'])
        
        for _, plane in aircraft.iterrows():
            session.add(Aircraft(
                tailnum=plane['tailnum'],
                carrier=plane['carrier']
            ))
        
        session.commit()
        print(f"Successfully inserted {len(aircraft)} aircraft")
        
        # 4. Insert Date Dimension
        print("Inserting Date Dimension...")
        dates = df[['year', 'month', 'day', 'day_of_week']].drop_duplicates()
        date_mapping = {}  # To store date_id for each date
        
        for idx, date in dates.iterrows():
            year = int(date['year'])
            month = int(date['month'])
            day = int(date['day'])
            
            try:
                full_date = datetime(year, month, day).date()
                date_entry = DateDimension(
                    year=year,
                    month=month,
                    day=day,
                    day_of_week=date['day_of_week'],
                    full_date=full_date
                )
                session.add(date_entry)
                session.flush()  # Get the ID without committing
                
                # Store date_id for lookup when inserting flights
                date_key = (year, month, day)
                date_mapping[date_key] = date_entry.date_id
                
            except ValueError as e:
                print(f"Invalid date: {year}-{month}-{day}, Error: {e}")
        
        session.commit()
        print(f"Successfully inserted {len(dates)} dates")
        
        # 5. Insert Flights and Delays (batch processing to avoid memory issues)
        print("Inserting Flights and Delays...")
        batch_size = 5000
        total_rows = len(df)
        flights_inserted = 0
        delays_inserted = 0
        
        for start_idx in range(0, total_rows, batch_size):
            end_idx = min(start_idx + batch_size, total_rows)
            batch = df.iloc[start_idx:end_idx]
            
            for _, row in batch.iterrows():
                # Skip rows with missing required data
                if pd.isna(row['tailnum']) or pd.isna(row['origin']) or pd.isna(row['destination']):
                    continue
                
                # Get date_id from the mapping
                date_key = (int(row['year']), int(row['month']), int(row['day']))
                date_id = date_mapping.get(date_key)
                
                if date_id is None:
                    print(f"Warning: Date not found for {date_key}")
                    continue
                
                # Create flight record
                flight = Flights(
                    flight_num=int(row['flight_num']),
                    carrier=row['carrier'],
                    tailnum=row['tailnum'],
                    origin=row['origin'],
                    destination=row['destination'],
                    date_id=date_id,
                    dep_time=row['dep_time'] if not pd.isna(row['dep_time']) else None,
                    sched_dep_time=row['sched_dep_time'],
                    arr_time=row['arr_time'] if not pd.isna(row['arr_time']) else None,
                    sched_arr_time=row['sched_arr_time'],
                    air_time=row['air_time'] if not pd.isna(row['air_time']) else None,
                    distance=int(row['distance']) if not pd.isna(row['distance']) else None,
                    hour=int(row['hour']) if not pd.isna(row['hour']) else None,
                    minute=int(row['minute']) if not pd.isna(row['minute']) else None
                )
                
                session.add(flight)
                session.flush()  # Get the flight ID
                flights_inserted += 1
                
                # Create delay record if there are delay times
                if not pd.isna(row['dep_delay_time']) or not pd.isna(row['arr_delay_time']):
                    delay = Delays(
                        flight_id=flight.id,
                        dep_delay_time=float(row['dep_delay_time']) if not pd.isna(row['dep_delay_time']) else None,
                        reason_dep_delay=row['reason_dep_delay'] if 'reason_dep_delay' in row and not pd.isna(row['reason_dep_delay']) else None,
                        arr_delay_time=float(row['arr_delay_time']) if not pd.isna(row['arr_delay_time']) else None,
                        reason_arr_delay=row['reason_arr_delay'] if 'reason_arr_delay' in row and not pd.isna(row['reason_arr_delay']) else None
                    )
                    session.add(delay)
                    delays_inserted += 1
            
            # Commit batch
            session.commit()
            print(f"Processed {end_idx}/{total_rows} rows")
        
        print(f"Successfully inserted {flights_inserted} flights and {delays_inserted} delays")
        
    except Exception as e:
        session.rollback()
        print(f"Error: {e}")
        raise
    finally:
        session.close()

if __name__ == "__main__":
    try:
        populate_database()
        print("Database population completed successfully!")
    except Exception as e:
        print(f"An error occurred during database population: {e}")