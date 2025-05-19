import pandas as pd

datoteka = r"D:\fax_\treca_godina\skladistenje_rudarenje\SRP_Projekt\checkpoint1\flights.csv"

data = pd.read_csv(datoteka, encoding="utf-8")

# Prikaži prvih nekoliko redaka (zamjena za display())
print(data.head())  

print("Veličina skupa podataka:", data.shape)

print("Nazivi stupaca:", data.columns.tolist())

print("Nedostajuće vrijednosti po stupcu:")
print(data.isna().sum())

print("Jedinstvene vrijednosti po stupcu:")
for column in data.columns:
    print(f"{column}: {data[column].unique()[:10]} ...")

print(data.dtypes)

print("Frekvencije vrijednosti po stupcu:")
for column in data.columns:
    print(f"{column}:")
    print(data[column].value_counts(), "\n")
