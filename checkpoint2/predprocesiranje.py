import pandas as pd

# === KORAK 1: Učitaj podatke ===
flights_df = pd.read_csv("flights_najbolji.csv")
airports_df = pd.read_csv("world-airports.csv")

# === KORAK 2: Osnovno čišćenje ===
flights_df = flights_df.copy()
flights_df.columns = flights_df.columns.str.lower().str.replace(' ', '_')
flights_df = flights_df.dropna()
flights_df = flights_df.drop(columns=['time_hour'])
flights_df = flights_df.rename(columns={
    'dest': 'destination',
    'dep_delay': 'dep_delay_time',
    'arr_delay': 'arr_delay_time',
    'flight': 'flight_num',
    'name': 'airline_name'
})

# === KORAK 3: Formatiranje vremena ===
time_columns = ["dep_time", "sched_dep_time", "arr_time", "sched_arr_time"]
for col in time_columns:
    flights_df[col] = flights_df[col].astype(int).astype(str).str.zfill(4)
    flights_df[col] = flights_df[col].str[:2] + ":" + flights_df[col].str[2:]

# === KORAK 4: Učitavanje i obrada podataka o aerodromima ===
airports_df = airports_df[['iata_code', 'municipality', 'country_name', 'name']]
airports_df = airports_df.rename(columns={
    'iata_code': 'IATA',
    'municipality': 'city',
    'country_name': 'country',
    'name': 'airport_name'
})

# === KORAK 5: Spajanje podataka za origin (polazište) ===
flights_df = flights_df.merge(
    airports_df,
    how='left',
    left_on='origin',
    right_on='IATA'
)
flights_df = flights_df.rename(columns={
    'city': 'departure_city',
    'country': 'departure_country',
    'airport_name': 'departure_airport_name'
})
flights_df = flights_df.drop(columns=['IATA'])

# === KORAK 6: Spajanje podataka za destination (odredište) ===
flights_df = flights_df.merge(
    airports_df,
    how='left',
    left_on='destination',
    right_on='IATA'
)

flights_df = flights_df.rename(columns={
    'city': 'destination_city',
    'country': 'destination_country',
    'airport_name': 'destination_airport_name'
})
flights_df = flights_df.drop(columns=['IATA'])

# === KORAK 7: Provjera nedostajućih podataka ===
# missing_origins = flights_df[flights_df['departure_city'].isna()]['origin'].unique()
# missing_dests = flights_df[flights_df['destination_city'].isna()]['destination'].unique()

# print("❗ Nedostaju podaci za origin kodove:", missing_origins)
# print("❗ Nedostaju podaci za destination kodove:", missing_dests)

# === KORAK 8: Podjela na 80/20 ===
df20 = flights_df.sample(frac=0.2, random_state=1)
df80 = flights_df.drop(df20.index)

print(" CSV size nakon obrade (80%):", df80.shape)
print(" CSV size (20%):", df20.shape)

# === KORAK 9: Spremanje datoteka ===
df80.to_csv("flights_najbolji_PROCESSED.csv", index=False)
df20.to_csv("flights_najbolji_PROCESSED_20.csv", index=False)

print(" Podaci su obrađeni i spremljeni u nove datoteke.")
