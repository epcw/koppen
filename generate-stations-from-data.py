#use this file if you need to spit out a list of station IDs from an existing koppen_class.csv (for example, to run the same list of stations on different years).

import pandas as pd
import csv

df = pd.read_csv('data/koppen_class.csv', dtype={'year': str})
df = df.dropna(subset=['koppen'])
df = df[(df['koppen'] != 'H')] #filter out specific koppen classes
df = df[['station']].drop_duplicates()

df_filename = 'stations/station_ids-generated.txt'
print("exporting " + df_filename)
df.to_csv(df_filename, index = False, quoting=csv.QUOTE_NONE, header = False)