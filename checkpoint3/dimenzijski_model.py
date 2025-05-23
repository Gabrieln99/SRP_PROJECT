from sqlalchemy import create_engine, Column, Integer, Float, String, BigInteger, ForeignKey
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker

DATABASE_URL = "mysql+pymysql://root:root@localhost:3306/dimenzijski_model"  # Ispravka: schema name
engine = create_engine(DATABASE_URL, echo=True)
Session = sessionmaker(bind=engine)
session = Session()

Base = declarative_base()

# ----------------------
# DIMENZIJE
# ----------------------

class DimAirline(Base):
    __tablename__ = 'dim_airline'
    airline_tk = Column(BigInteger, primary_key=True, autoincrement=True)  # Dodao autoincrement
    carrier = Column(String(3), nullable=False)
    airline_name = Column(String(100), nullable=False)

class DimAircraft(Base):
    __tablename__ = 'dim_aircraft'
    aircraft_tk = Column(BigInteger, primary_key=True, autoincrement=True)  # Dodao autoincrement
    tailnum = Column(String(10), nullable=False)

class DimRoute(Base):
    __tablename__ = 'dim_route'
    route_tk = Column(BigInteger, primary_key=True, autoincrement=True)  # Dodao autoincrement
    origin = Column(String(5), nullable=False)
    destination = Column(String(5), nullable=False)
    departure_city = Column(String(50))
    departure_country = Column(String(50))
    departure_airport_name = Column(String(100))
    destination_city = Column(String(50))
    destination_country = Column(String(50))
    destination_airport_name = Column(String(100))
    distance = Column(Float)

class DimDepDelay(Base):
    __tablename__ = 'dim_dep_delay'
    dep_delay_tk = Column(BigInteger, primary_key=True, autoincrement=True)  # Dodao autoincrement
    reason = Column(String(50))
    delay_time = Column(Float)

class DimArrDelay(Base):
    __tablename__ = 'dim_arr_delay'
    arr_delay_tk = Column(BigInteger, primary_key=True, autoincrement=True)  # Dodao autoincrement
    reason = Column(String(50))
    delay_time = Column(Float)

class DimDate(Base):
    __tablename__ = 'dim_date'
    date_tk = Column(BigInteger, primary_key=True, autoincrement=True)  # Dodao autoincrement
    year = Column(Integer)
    month = Column(Integer)
    day = Column(Integer)
    day_of_week = Column(String(10))
    # DODANO: Korisni datumski atributi za lakše analize
    quarter = Column(Integer)  # Q1, Q2, Q3, Q4
    month_name = Column(String(15))  # January, February, etc.
    is_weekend = Column(Integer)  # 0/1 flag

class DimTime(Base):
    __tablename__ = 'dim_time'
    time_tk = Column(BigInteger, primary_key=True, autoincrement=True)  # Dodao autoincrement
    dep_time = Column(String(10))
    arr_time = Column(String(10))
    sched_dep_time = Column(String(10))
    sched_arr_time = Column(String(10))
    hour = Column(Integer)
    minute = Column(Integer)
    # DODANO: Korisni vremenski atributi
    time_of_day = Column(String(20))  # Morning, Afternoon, Evening, Night
    is_peak_hour = Column(Integer)  # 0/1 flag za rush hour

# ----------------------
# ČINJENICA
# ----------------------

class FactFlight(Base):
    __tablename__ = 'fact_flight'
    flight_tk = Column(BigInteger, primary_key=True, autoincrement=True)  # Dodao autoincrement
    
    # Foreign Keys (Dimension References)
    airline_id = Column(BigInteger, ForeignKey('dim_airline.airline_tk'))
    aircraft_id = Column(BigInteger, ForeignKey('dim_aircraft.aircraft_tk'))
    route_id = Column(BigInteger, ForeignKey('dim_route.route_tk'))
    dep_delay_id = Column(BigInteger, ForeignKey('dim_dep_delay.dep_delay_tk'))
    arr_delay_id = Column(BigInteger, ForeignKey('dim_arr_delay.arr_delay_tk'))
    date_id = Column(BigInteger, ForeignKey('dim_date.date_tk'))
    time_id = Column(BigInteger, ForeignKey('dim_time.time_tk'))
    
    # Measures (Metrics for Analysis)
    air_time = Column(Float)
    flight_num = Column(Integer)
    # DODANO: Dodatne mere za analizu
    scheduled_duration = Column(Float)  # Planirano trajanje leta
    actual_duration = Column(Float)     # Stvarno trajanje leta
    on_time_flag = Column(Integer)      # 0/1 da li je let bio na vreme
    
# ----------------------
# KREIRANJE TABLICA
# ----------------------

# Kreiranje baze podataka ako ne postoji
from sqlalchemy import text
with engine.connect() as conn:
    conn.execute(text("CREATE DATABASE IF NOT EXISTS dimenzijski_model"))
    conn.commit()

Base.metadata.create_all(engine)
print("Dimenzijski model kreiran u bazi 'dimenzijski_model'.")

# Prikaz strukture
print("\n=== DIMENSIONAL MODEL STRUKTURA ===")
print("DIMENZIJE:")
print("- dim_airline (aviokompanija)")
print("- dim_aircraft (avion)")  
print("- dim_route (ruta)")
print("- dim_dep_delay (kašnjenje polaska)")
print("- dim_arr_delay (kašnjenje dolaska)")
print("- dim_date (datum)")
print("- dim_time (vrijeme)")
print("\nFACT TABELA:")
print("- fact_flight (činjenica o letu)")

print(f"\nUkupno tabela: {len(Base.metadata.tables)}")