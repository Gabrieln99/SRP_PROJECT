# Imports
import pandas as pd
import json
import requests
import random
from sqlalchemy import create_engine, Column, Integer, String, Float, ForeignKey
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base


# Putanja do predprocesirane CSV datoteke (80% podataka)
CSV_FILE_PATH = "flights_processed_80.csv"

# Učitavanje CSV datoteke u dataframe
df = pd.read_csv(CSV_FILE_PATH, delimiter=',')
print(f"CSV size: {df.shape}")  # Print dataset size
print(df.head())  # Preview first few rows

# Database Connection
Base = declarative_base()

# Definiranje sheme baze podataka
# --------------------------------------------------------------
class Airline(Base):
    __tablename__ = 'airline'
    id = Column(Integer, primary_key=True, autoincrement=True)
    carrier = Column(String(3), nullable=False, unique=True)
    airline_name = Column(String(100), nullable=False)

class Aircraft(Base):
    __tablename__ = 'aircraft'
    id = Column(Integer, primary_key=True, autoincrement=True)
    tailnum = Column(String(10), nullable=False, unique=True)
    airline_fk = Column(Integer, ForeignKey('airline.id'))

class Route(Base):
    __tablename__ = 'route'
    id = Column(Integer, primary_key=True, autoincrement=True)
    origin = Column(String(5), nullable=False)
    destination = Column(String(5), nullable=False)
    departure_city = Column(String(50))
    departure_country = Column(String(50))
    departure_airport_name = Column(String(100))
    destination_city = Column(String(50))
    destination_country = Column(String(50))
    destination_airport_name = Column(String(100))
    distance = Column(Float)

class DepDelay(Base):
    __tablename__ = 'dep_delay'
    id = Column(Integer, primary_key=True, autoincrement=True)
    reason_dep_delay = Column(String(50))
    dep_delay_time = Column(Float)  # VRAĆENO za lakše vizualizacije!

class ArrDelay(Base):
    __tablename__ = 'arr_delay'
    id = Column(Integer, primary_key=True, autoincrement=True)
    reason_arr_delay = Column(String(50))
    arr_delay_time = Column(Float)  # VRAĆENO za lakše vizualizacije!

class Flight(Base):
    __tablename__ = 'flight'
    id = Column(Integer, primary_key=True, autoincrement=True)
    year = Column(Integer, nullable=False)
    month = Column(Integer, nullable=False)
    day = Column(Integer, nullable=False)
    day_of_week = Column(String(10), nullable=False)
    dep_time = Column(String(10))
    arr_time = Column(String(10))
    sched_dep_time = Column(String(10), nullable=False)
    sched_arr_time = Column(String(10), nullable=False)
    hour = Column(Integer)
    minute = Column(Integer)
    air_time = Column(Float)
    flight_num = Column(Integer)
    dep_delay_fk = Column(Integer, ForeignKey('dep_delay.id'))
    arr_delay_fk = Column(Integer, ForeignKey('arr_delay.id'))
    route_fk = Column(Integer, ForeignKey('route.id'))
    aircraft_fk = Column(Integer, ForeignKey('aircraft.id'))

# Database Connection
engine = create_engine('mysql+pymysql://root:root@localhost:3306/flights_dw', echo=False)
Base.metadata.drop_all(engine)  # Brisanje postojećih tablica
Base.metadata.create_all(engine)  # Stvaranje tablica

Session = sessionmaker(bind=engine)  # Stvaranje sesije
session = Session()  # Otvori novu sesiju

# --------------------------------------------------------------
# Import podataka
# --------------------------------------------------------------

print("Početak importa podataka...")

# **1. Umetanje aviokompanije**
airlines = df[['carrier', 'airline_name']].drop_duplicates()  # Dohvatimo jedinstvene aviokompanije
airlines_list = airlines.to_dict(orient="records")  # Pretvori u listu rječnika

session.bulk_insert_mappings(Airline, airlines_list)  # Bulk insert
session.commit()

airline_map = {a.carrier: a.id for a in session.query(Airline).all()}  # Stvori mapiranje aviokompanija
print(f"Uneseno {len(airlines_list)} aviokompanija")

# **2. Umetanje ruta**
routes = df[['origin', 'destination', 'departure_city', 'departure_country', 
             'departure_airport_name', 'destination_city', 'destination_country', 
             'destination_airport_name', 'distance']].drop_duplicates()  # Dohvatimo jedinstvene rute

routes_list = routes.to_dict(orient="records")  # Pretvori u listu rječnika

session.bulk_insert_mappings(Route, routes_list)  # Bulk insert
session.commit()

# Kreiraj mapiranje ruta - kombinacija origin i destination
route_map = {}
for route in session.query(Route).all():
    key = f"{route.origin}_{route.destination}"
    route_map[key] = route.id

print(f"Uneseno {len(routes_list)} ruta")

# **3. Umetanje razloga kašnjenja polaska**
dep_delays = df[['reason_dep_delay', 'dep_delay_time']].drop_duplicates()  # Kombinacija razlog + vreme
dep_delays_list = dep_delays.to_dict(orient="records")

session.bulk_insert_mappings(DepDelay, dep_delays_list)
session.commit()

# Kreiraj mapiranje kašnjenja polaska - kombinacija razloga i vremena
dep_delay_map = {}
for delay in session.query(DepDelay).all():
    key = f"{delay.reason_dep_delay}_{delay.dep_delay_time}"
    dep_delay_map[key] = delay.id

print(f"Uneseno {len(dep_delays_list)} kombinacija razlog+vreme kašnjenja polaska")

# **4. Umetanje razloga kašnjenja dolaska**
arr_delays = df[['reason_arr_delay', 'arr_delay_time']].drop_duplicates()  # Kombinacija razlog + vreme
arr_delays_list = arr_delays.to_dict(orient="records")

session.bulk_insert_mappings(ArrDelay, arr_delays_list)
session.commit()

# Kreiraj mapiranje kašnjenja dolaska - kombinacija razloga i vremena
arr_delay_map = {}
for delay in session.query(ArrDelay).all():
    key = f"{delay.reason_arr_delay}_{delay.arr_delay_time}"
    arr_delay_map[key] = delay.id

print(f"Uneseno {len(arr_delays_list)} kombinacija razlog+vreme kašnjenja dolaska")

# **5. Umetanje aviona**
# Prvo trebamo kreirati mapiranje aviokompanija prema carrier kodu
aircrafts = df[['tailnum', 'carrier']].drop_duplicates()  # Dohvatimo jedinstvene avione

# DODATNO: Ukloni duplikate tailnum-a (isti avion može leteti za različite kompanije u različitim periodima)
# Zadržaj prvi pojavak svakog tailnum-a
aircrafts = aircrafts.drop_duplicates(subset=['tailnum'], keep='first')

aircrafts['airline_fk'] = aircrafts['carrier'].map(airline_map)  # Mapiraj aviokompaniju iz koda u ID
aircrafts_final = aircrafts.drop(columns=['carrier']).to_dict(orient="records")  # Izbaci carrier stupac

session.bulk_insert_mappings(Aircraft, aircrafts_final)  # Bulk insert
session.commit()

aircraft_map = {a.tailnum: a.id for a in session.query(Aircraft).all()}  # Stvori mapiranje aviona
print(f"Uneseno {len(aircrafts_final)} aviona")

# **6. Umetanje letova**
flights_data = df[['year', 'month', 'day', 'day_of_week', 'dep_time', 'arr_time',
                   'sched_dep_time', 'sched_arr_time', 'hour', 'minute', 'air_time', 
                   'flight_num', 'origin', 'destination', 'tailnum', 
                   'reason_dep_delay', 'dep_delay_time', 'reason_arr_delay', 
                   'arr_delay_time']].copy()  # Dohvati potrebne stupce

# Mapiraj strane ključeve
flights_data['route_fk'] = flights_data.apply(lambda row: route_map[f"{row['origin']}_{row['destination']}"], axis=1)
flights_data['aircraft_fk'] = flights_data['tailnum'].map(aircraft_map)
flights_data['dep_delay_fk'] = flights_data.apply(lambda row: dep_delay_map[f"{row['reason_dep_delay']}_{row['dep_delay_time']}"], axis=1)
flights_data['arr_delay_fk'] = flights_data.apply(lambda row: arr_delay_map[f"{row['reason_arr_delay']}_{row['arr_delay_time']}"], axis=1)

# Ukloni originalne stupce koji više nisu potrebni
flights_final = flights_data.drop(columns=['origin', 'destination', 'tailnum', 
                                          'reason_dep_delay', 'dep_delay_time', 
                                          'reason_arr_delay', 'arr_delay_time']).to_dict(orient="records")

session.bulk_insert_mappings(Flight, flights_final)  # Bulk insert
session.commit()

print(f"Uneseno {len(flights_final)} letova")

print("Data imported successfully!")
print(f"Ukupno tabela: {len(Base.metadata.tables)}")
print("Tabele:")
for table_name in Base.metadata.tables.keys():
    print(f"- {table_name}")

session.close()  # Zatvori sesiju
