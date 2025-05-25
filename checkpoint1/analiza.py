import pandas as pd
import os

# Definirali smo relativnu putanju do datoteke
datoteka = os.path.join(os.path.dirname(__file__), "flights_main.csv")

# Učitali smo podatke iz CSV datoteke
data = pd.read_csv(datoteka, encoding="utf-8")

# Prikazali smo osnovne informacije o podacima
print(data.head())
print("Veličina skupa podataka:", data.shape)
print("Nazivi stupaca:", data.columns.tolist())

# Provjerili smo nedostajuće vrijednosti
print("Nedostajuće vrijednosti po stupcu:")
print(data.isna().sum())

# Ispisali smo jedinstvene vrijednosti za svaki stupac
print("Jedinstvene vrijednosti po stupcu:")
for column in data.columns:
   print(f"{column}: {data[column].unique()[:10]} ...")

# Provjerili smo tipove podataka
print(data.dtypes)

# Analizirali smo frekvencije vrijednosti po stupcima
print("Frekvencije vrijednosti po stupcu:")
for column in data.columns:
   print(f"{column}:")
   print(data[column].value_counts(), "\n")