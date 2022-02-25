#RUN THIS LAST - only once you have all your data pulled and in /data
#THIS SCRIPT takes data pulled from NOAA_data_pull.py and then assigns a koppen climate code to each station in the dataset for each year that data exists

import pandas as pd
import csv

df_filename = 'data/koppen_class.csv'

df = pd.read_csv('data/station_data_test.csv')
df['date'] = pd.to_datetime(df['date']) #converts the date string to datetime
#df['date'] = df['date'].dt.date #drops the midnight timestamp - comment out if this is actually meaningful for you

df_station_list = pd.read_csv('stations/station_list.csv')
df_station_list = df_station_list[['elevation','mindate','maxdate','latitude','longitude','name','id']]
df_station_list = df_station_list.rename(columns = {'id' : 'station'})

df = df.merge(df_station_list, how = 'inner', left_on = ['station'], right_on = ['station'])

winter_months = [1,2,3,10,11,12] #from Koppen climate classification for N hemisphere
df['year_half'] = 'summer'
df.loc[df['date'].dt.month.isin(winter_months), 'year_half'] = 'winter'

df['year'] = df['date'].dt.year

#calculate temperature metrics per station per year
df_temp = df[(df['datatype'] == 'TAVG')]
df_temp = df_temp[['date', 'year','datatype', 'station', 'value']]
df_temp['num_mo_btw_10-22C'] = 0
df_temp.loc[(df['value'] > 10) & (df_temp['value'] < 22), 'num_mo_btw_10-22C'] = 1
df_agg = df_temp.groupby(['station','year'])[['num_mo_btw_10-22C']].sum().reset_index()
df_temp_agg = df_temp.groupby(['station','year'])[['value']].min().reset_index()
df_temp_agg = df_temp_agg.rename(columns = {'value' : 'temp_coolest_mo'})
df_agg = df_agg.merge(df_temp_agg, how = "outer", left_on = ['station','year'], right_on = ['station','year'])
df_temp_agg = df_temp.groupby(['station','year'])[['value']].max().reset_index()
df_temp_agg = df_temp_agg.rename(columns = {'value' : 'temp_hottest_mo'})
df_agg = df_agg.merge(df_temp_agg, how = "outer", left_on = ['station','year'], right_on = ['station','year'])
df_temp_agg = df_temp.groupby(['station','year'])[['value']].mean().reset_index()
df_temp_agg = df_temp_agg.rename(columns = {'value' : 'avg_annual_temp(t)'})
df_agg = df_agg.merge(df_temp_agg, how = "outer", left_on = ['station','year'], right_on = ['station','year'])
del_list = [df_temp, df_temp_agg]
del del_list
del df_temp
del df_temp_agg

#calculate precipitation metrics per station per year
df_prcp = df[(df['datatype'] == 'PRCP')]
df_prcp = df_prcp[['date', 'year','datatype', 'station', 'value', 'year_half']]
df_prcp_agg = df_prcp.groupby(['station','year'])[['value']].min().reset_index()
df_prcp_agg = df_prcp_agg.rename(columns = {'value' : 'prcp_driest_mo'})
df_agg = df_agg.merge(df_prcp_agg, how = "outer", left_on = ['station','year'], right_on = ['station','year'])
df_prcp_agg = df_prcp.groupby(['station','year'])[['value']].max().reset_index()
df_prcp_agg = df_prcp_agg.rename(columns = {'value' : 'prcp_wettest_mo'})
df_agg = df_agg.merge(df_prcp_agg, how = "outer", left_on = ['station','year'], right_on = ['station','year'])
df_prcp_agg = df_prcp.groupby(['station','year'])[['value']].sum().reset_index()
df_prcp_agg = df_prcp_agg.rename(columns = {'value' : 'annual_prcp(r)'})
df_agg = df_agg.merge(df_prcp_agg, how = "outer", left_on = ['station','year'], right_on = ['station','year'])
df_prcp_summer = df_prcp[(df_prcp['year_half'] == 'summer')]
df_prcp_summer_agg = df_prcp_summer.groupby(['station','year'])[['value']].sum().reset_index()
df_prcp_summer_agg = df_prcp_summer_agg.rename(columns = {'value' : 'prcp_summer'})
df_agg = df_agg.merge(df_prcp_summer_agg, how = "outer", left_on = ['station','year'], right_on = ['station','year'])
df_prcp_summer_agg = df_prcp_summer.groupby(['station','year'])[['value']].min().reset_index()
df_prcp_summer_agg = df_prcp_summer_agg.rename(columns = {'value' : 'prcp_driest_mo_summer'})
df_agg = df_agg.merge(df_prcp_summer_agg, how = "outer", left_on = ['station','year'], right_on = ['station','year'])
df_prcp_summer_agg = df_prcp_summer.groupby(['station','year'])[['value']].max().reset_index()
df_prcp_summer_agg = df_prcp_summer_agg.rename(columns = {'value' : 'prcp_wettest_mo_summer'})
df_agg = df_agg.merge(df_prcp_summer_agg, how = "outer", left_on = ['station','year'], right_on = ['station','year'])
df_prcp_winter = df_prcp[(df_prcp['year_half'] == 'winter')]
df_prcp_winter_agg = df_prcp_winter.groupby(['station','year'])[['value']].sum().reset_index()
df_prcp_winter_agg = df_prcp_winter_agg.rename(columns = {'value' : 'prcp_winter'})
df_agg = df_agg.merge(df_prcp_winter_agg, how = "outer", left_on = ['station','year'], right_on = ['station','year'])
df_prcp_winter_agg = df_prcp_winter.groupby(['station','year'])[['value']].min().reset_index()
df_prcp_winter_agg = df_prcp_winter_agg.rename(columns = {'value' : 'prcp_driest_mo_winter'})
df_agg = df_agg.merge(df_prcp_winter_agg, how = "outer", left_on = ['station','year'], right_on = ['station','year'])
df_prcp_winter_agg = df_prcp_winter.groupby(['station','year'])[['value']].max().reset_index()
df_prcp_winter_agg = df_prcp_winter_agg.rename(columns = {'value' : 'prcp_wettest_mo_winter'})
df_agg = df_agg.merge(df_prcp_winter_agg, how = "outer", left_on = ['station','year'], right_on = ['station','year'])
del_list = [df, df_prcp_winter_agg, df_prcp_winter, df_prcp_summer, df_prcp_summer_agg, df_prcp_agg, df_prcp]
del del_list
del df
del df_prcp_winter_agg
del df_prcp_winter
del df_prcp_summer
del df_prcp_summer_agg
del df_prcp_agg
del df_prcp

#TODO: KOPPEN CLASS SEGMENTING BASED ON ABOVE DF_AGG

#bring in the station location data
df_agg = df_agg.merge(df_station_list, how="left", left_on = ['station'], right_on = ['station'])

df_agg.to_csv(df_filename, mode='a', index=False, quotechar='"', quoting=csv.QUOTE_ALL, header=True)
print("exporting " + df_filename)

#print(df)