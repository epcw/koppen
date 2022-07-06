import pandas as pd
import csv

df = pd.read_csv('data/koppen_class.csv', dtype={'year': str})
df = df.dropna(subset=['koppen'])
df = df[(df['koppen'] != 'H')] #filter out specific koppen classes
df = df[['station','name','year','years_averaged','latitude','longitude','elevation','koppen','koppen_name']].drop_duplicates()
df = df.rename(columns = {'station':'noaa'})
df['name'] = df['name'].str.replace(r' US$', '')

# make a list of timescales to loop over
timescales = df['years_averaged'].unique()

for t in timescales:
    df_filename = 'map/stations_' + str(t) + '.csv'
    df_temp = df[(df['years_averaged'] == t)]
    df_temp = df_temp[['noaa', 'name', 'year', 'latitude', 'longitude', 'elevation', 'koppen',
             'koppen_name']].drop_duplicates()
    print("exporting " + df_filename)

    df_temp.to_csv(df_filename, index=False, quotechar='"', quoting=csv.QUOTE_ALL)
    df_edges = df_temp[['noaa', 'name', 'year']]
    df_edges['origin'] = df_edges['noaa']
    df_edges['destination'] = df_edges['noaa']
    df_edges['count'] = 1

    df_edges_filename = 'map/edges_' + str(t) + '.csv'
    print("exporting " + df_edges_filename)
    df_edges.to_csv(df_edges_filename, index=False, quotechar='"', quoting=csv.QUOTE_ALL)