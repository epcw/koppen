import pandas as pd
import csv

df = pd.read_csv('data/koppen_class.csv')
df = df.dropna(subset=['koppen'])
df = df[['station','name','latitude','longitude','koppen','koppen_name']].drop_duplicates()

df_filename = 'map/stations.csv'
print("exporting " + df_filename)
df.to_csv(df_filename, index = False, quotechar='"',quoting=csv.QUOTE_ALL)
