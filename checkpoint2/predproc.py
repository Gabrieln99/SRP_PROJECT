import pandas as pd

# Odredili smo putanju do CSV datoteke
CSV_FILE_PATH = "flights_main.csv"

# Učitali smo podatke i ispisali dimenzije
df = pd.read_csv(CSV_FILE_PATH, delimiter=',')
print("CSV size before: ", df.shape)

# Standardizirali smo nazive zemalja
df['departure_country'] = df['departure_country'].replace('USA', 'United States')
df['destination_country'] = df['destination_country'].replace('USA', 'United States') 
df['departure_country'] = df['departure_country'].replace('UK', 'United Kingdom')
df['destination_country'] = df['destination_country'].replace('UK', 'United Kingdom')

# Uklonili smo redove s nedostajućim vrijednostima
df = df.dropna()

# Pretvorili smo nazive stupaca u mala slova
df.columns = df.columns.str.lower()

# Zamijenili smo razmake u nazivima stupaca s donjom crtom
df.columns = df.columns.str.replace(' ', '_')

print("CSV size after: ", df.shape)
print(df.head())

# Provjerili smo postojanje duplikata
duplicates = df.duplicated().sum()
print(f"Number of duplicates: {duplicates}")

# Uklonili smo duplikate ako postoje
if duplicates > 0:
   df = df.drop_duplicates()
   print(f"CSV size after removing duplicates: {df.shape}")

# Podijelili smo podatke na 80:20
df20 = df.sample(frac=0.2, random_state=1)
df80 = df.drop(df20.index)

print("CSV size 80: ", df80.shape)
print("CSV size 20: ", df20.shape)

# Spremili smo obrađene podatke u nove datoteke
df80.to_csv("flights_processed_80.csv", index=False)
df20.to_csv("flights_processed_20.csv", index=False)

print("\nPredprocesiranje završeno!")
print("Stvorene datoteke:")
print("- flights_processed_80.csv (80% podataka za transakcijski model)")
print("- flights_processed_20.csv (20% podataka za ETL direktno iz CSV-a)")

# Prikazali smo osnovne statistike
print("\nOsnovne statistike:")
print(f"Ukupno letova: {len(df)}")
print(f"Broj različitih aerodroma polaska: {df['origin'].nunique()}")
print(f"Broj različitih aerodroma dolaska: {df['destination'].nunique()}")
print(f"Broj različitih prijevoznika: {df['carrier'].nunique()}")
print(f"Raspon godina: {df['year'].min()} - {df['year'].max()}")