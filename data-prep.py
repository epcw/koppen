import pandas as pd
import csv

df = pd.read_csv('data/koppen_class.csv', dtype={'year': str})
df = df.dropna(subset=['koppen'])
df = df[(df['koppen'] != 'H')] #filter out specific koppen classes
df = df[['station','name','year','tag','latitude','longitude','elevation','koppen','koppen_name']].drop_duplicates()
df = df.rename(columns = {'station':'noaa'})
df['name'] = df['name'].str.replace(r' US$', '')

# # make a list of timescales to loop over
# tag = df['tag'].unique()
#
# stations = df['noaa'].unique()

# for s in stations:
#         dftemp = df[(df['years_averaged'] == t)]
#         dftemp = dftemp[(dftemp['year'] == dftemp['year'].max())]
#         # dftemp = dftemp[dftemp('year') == dftemp('year').max()]
#         df_scale = pd.concat([df_scale, dftemp])

stations_filename = 'map/stations_weighted.csv'
df_stations = df[['noaa', 'name', 'year', 'tag', 'latitude', 'longitude', 'elevation', 'koppen', 'koppen_name']].drop_duplicates()
print("exporting " + stations_filename)
df_stations.to_csv(stations_filename, index=False, quotechar='"', quoting=csv.QUOTE_ALL)

df_edges = df[['noaa', 'name', 'year']]
df_edges['origin'] = df_edges['noaa']
df_edges['destination'] = df_edges['noaa']
df_edges['count'] = 1

df_edges_filename = 'map/edges_weighted.csv'
print("exporting " + df_edges_filename)
df_edges.to_csv(df_edges_filename, index=False, quotechar='"', quoting=csv.QUOTE_ALL)

# df_scale = pd.DataFrame()
#
# timescales_mod = timescales[1:]
#
# for s in stations:
#     for t in timescales_mod:
#         dftemp = df[(df['years_averaged'] == t)]
#         dftemp = dftemp[(dftemp['year'] == dftemp['year'].max())]
#         # dftemp = dftemp[dftemp('year') == dftemp('year').max()]
#         df_scale = pd.concat([df_scale, dftemp])
#
# df_scale_filename = 'map/stations_scale.csv'
# df_scale = df_scale[['noaa', 'name', 'year', 'latitude', 'longitude', 'elevation', 'koppen', 'koppen_name']].drop_duplicates()
# print("exporting " + df_scale_filename)
# df_scale.to_csv(df_scale_filename, index=False, quotechar='"', quoting=csv.QUOTE_ALL)
#
# df_scale_edges = df_scale[['noaa', 'name', 'year']]
# df_scale_edges['origin'] = df_scale_edges['noaa']
# df_scale_edges['destination'] = df_scale_edges['noaa']
# df_scale_edges['count'] = 1
#
# df_scale_edges_filename = 'map/edges_scale.csv'
# print("exporting " + df_scale_edges_filename)
# df_scale_edges.to_csv(df_scale_edges_filename, index=False, quotechar='"', quoting=csv.QUOTE_ALL)

# for t in tag:
#     df_filename = 'map/stations_' + str(t) + '.csv'
#     df_temp = df[(df['tag'] == t)]
#     df_temp = df_temp[['noaa', 'name', 'year', 'latitude', 'longitude', 'elevation', 'koppen','koppen_name']].drop_duplicates()
#     print("exporting " + df_filename)
#
#     df_temp.to_csv(df_filename, index=False, quotechar='"', quoting=csv.QUOTE_ALL)
#     df_edges = df_temp[['noaa', 'name', 'year']]
#     df_edges['origin'] = df_edges['noaa']
#     df_edges['destination'] = df_edges['noaa']
#     df_edges['count'] = 1
#
#     df_edges_filename = 'map/edges_' + str(t) + '.csv'
#     print("exporting " + df_edges_filename)
#     df_edges.to_csv(df_edges_filename, index=False, quotechar='"', quoting=csv.QUOTE_ALL)