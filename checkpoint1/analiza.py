import pandas as pd
import os

# === KORAK 1: Definiraj relativnu putanju do datoteke ===

datoteka = os.path.join(os.path.dirname(__file__), "flights.csv")

# === KORAK 2: Učitaj podatke ===
data = pd.read_csv(datoteka, encoding="utf-8")

# === KORAK 3: Prikazi osnovne informacije ===
print(data.head())  
print("Veličina skupa podataka:", data.shape)
print("Nazivi stupaca:", data.columns.tolist())

# === KORAK 4: Nedostajuće vrijednosti ===
print("Nedostajuće vrijednosti po stupcu:")
print(data.isna().sum())

# === KORAK 5: Jedinstvene vrijednosti po stupcu (prvih 10) ===
print("Jedinstvene vrijednosti po stupcu:")
for column in data.columns:
    print(f"{column}: {data[column].unique()[:10]} ...")

# === KORAK 6: Tipovi podataka ===
print(data.dtypes)

# === KORAK 7: Frekvencije vrijednosti po stupcu ===
print("Frekvencije vrijednosti po stupcu:")
for column in data.columns:
    print(f"{column}:")
    print(data[column].value_counts(), "\n")
