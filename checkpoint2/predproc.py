import pandas as pd
"""
2. Skripta za predprocesiranje skupa podataka - LETOVI
Predprocesiranje skupa podataka o letovima
"""

# Određivanje putanje do CSV datoteke
CSV_FILE_PATH = "flights_main.csv"

# Učitavanje CSV datoteke (provjerite svoje delimiter u csv datoteci), ispis broja redaka i stupaca
df = pd.read_csv(CSV_FILE_PATH, delimiter=',')
print("CSV size before: ", df.shape)

# Standardizacija naziva zemalja (ako je potrebno)
df['departure_country'] = df['departure_country'].replace('USA', 'United States')
df['destination_country'] = df['destination_country'].replace('USA', 'United States') 
df['departure_country'] = df['departure_country'].replace('UK', 'United Kingdom')
df['destination_country'] = df['destination_country'].replace('UK', 'United Kingdom')

# Brisanje redaka s nedostajućim vrijednostima
df = df.dropna()

# Pretvori sve nazive stupaca u mala slova
df.columns = df.columns.str.lower()

# Zamjena razmaka u nazivima stupaca s donjom crtom
df.columns = df.columns.str.replace(' ', '_')

print("CSV size after: ", df.shape)  # Ispis broja redaka i stupaca nakon predprocesiranja
print(df.head())  # Ispis prvih redaka dataframe-a

# Count if there are duplicates
duplicates = df.duplicated().sum()
print(f"Number of duplicates: {duplicates}")  # Ispis broja duplikata

# Uklanjanje duplikata ako postoje
if duplicates > 0:
    df = df.drop_duplicates()
    print(f"CSV size after removing duplicates: {df.shape}")

# Random dijeljenje skupa podataka na dva dijela 80:20 (trebat će nam kasnije)
df20 = df.sample(frac=0.2, random_state=1)
df80 = df.drop(df20.index)

print("CSV size 80: ", df80.shape)
print("CSV size 20: ", df20.shape)

# Spremanje predprocesiranog skupa podataka u novu CSV datoteku
df80.to_csv("flights_processed_80.csv", index=False)  # 80% podataka za transakcijski model
df20.to_csv("flights_processed_20.csv", index=False)  # 20% podataka za ETL iz CSV-a

print("\nPredprocesiranje završeno!")
print("Stvorene datoteke:")
print("- flights_processed_80.csv (80% podataka za transakcijski model)")
print("- flights_processed_20.csv (20% podataka za ETL direktno iz CSV-a)")

# Prikaz osnovnih statistika
print("\nOsnovne statistike:")
print(f"Ukupno letova: {len(df)}")
print(f"Broj različitih aerodroma polaska: {df['origin'].nunique()}")
print(f"Broj različitih aerodroma dolaska: {df['destination'].nunique()}")
print(f"Broj različitih prijevoznika: {df['carrier'].nunique()}")
print(f"Raspon godina: {df['year'].min()} - {df['year'].max()}")