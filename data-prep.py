import pandas as pd
import csv

df = pd.read_csv('data/koppen_class.csv', dtype={'year': str})
df = df.dropna(subset=['koppen'])
df = df[(df['year'] == '2019')]
df = df[['station','name','year','latitude','longitude','elevation','koppen','koppen_name']].drop_duplicates()
df = df.rename(columns = {'station':'noaa'})

#"origin","destination","count","iata","order_count","order_length_days","date_start","date_end"

df_edges = df
df_edges['origin'] = df_edges['noaa']
df_edges['destination'] = df_edges['noaa']
df_edges['count'] = 1

df_filename = 'map/stations.csv'
df_edges_filename = 'map/edges.csv'
print("exporting " + df_filename)
df.to_csv(df_filename, index = False, quotechar='"',quoting=csv.QUOTE_ALL)
print("exporting " + df_edges_filename)
df_edges.to_csv(df_edges_filename, index = False, quotechar='"',quoting=csv.QUOTE_ALL)
