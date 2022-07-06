import pandas as pd
import csv

df_filename = 'stations/station_list_complete.csv'

# check and de-duplicate
df_check = pd.read_csv(df_filename)
df_check = df_check.drop_duplicates()

df_check.to_csv(df_filename, index=False, quotechar='"', quoting=csv.QUOTE_ALL, header=True)
print("exporting " + df_filename)
