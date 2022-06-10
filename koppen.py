# RUN THIS LAST - only once you have all your data pulled and in /data
# THIS SCRIPT takes data pulled from NOAA_data_pull.py and then assigns a koppen climate code to each station in the dataset for each year that data exists

import pandas as pd
import csv
import os

df_filename = 'data/koppen_class.csv'

df = pd.read_csv('data/station_data.csv')
df['date'] = pd.to_datetime(df['date'])  # converts the date string to datetime
# df['date'] = df['date'].dt.date #drops the midnight timestamp - comment out if this is actually meaningful for you

df_station_list = pd.read_csv('stations/station_list.csv')
df_station_list = df_station_list[['elevation', 'mindate', 'maxdate', 'latitude', 'longitude', 'name', 'id']]
df_station_list = df_station_list.rename(columns={'id': 'station'})

df = df.merge(df_station_list, how='inner', left_on=['station'], right_on=['station'])

winter_months = [1, 2, 3, 10, 11, 12]  # from Koppen climate classification for N hemisphere
df['year_half'] = 'summer'
df.loc[df['date'].dt.month.isin(winter_months), 'year_half'] = 'winter'

df['year'] = df['date'].dt.year

# calculate temperature metrics per station per year
df_temp = df[(df['datatype'] == 'TAVG')]
df_temp = df_temp[['date', 'year', 'datatype', 'station', 'value']]
df_temp['num_mo_btw_10-22C'] = 0
df_temp.loc[(df['value'] > 10) & (df_temp['value'] < 22), 'num_mo_btw_10-22C'] = 1
df_agg = df_temp.groupby(['station', 'year'])[['num_mo_btw_10-22C']].sum().reset_index()
df_temp_agg = df_agg
df_temp_agg_5 = df_agg.groupby(['station']).rolling(5,5, center=True,on='year').mean().reset_index()
df_temp_agg_5 = df_temp_agg_5.rename(columns={'num_mo_btw_10-22C' : 'num_mo_btw_10-22C_5yr'})
df_temp_agg_5 = df_temp_agg_5.drop(columns="level_1")
df_temp_agg = df_temp_agg.merge(df_temp_agg_5, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
df_temp_agg_10 = df_agg.groupby(['station']).rolling(10,10, center=True,on='year').mean().reset_index()
df_temp_agg_10 = df_temp_agg_10.rename(columns={'num_mo_btw_10-22C' : 'num_mo_btw_10-22C_10yr'})
df_temp_agg_10 = df_temp_agg_10.drop(columns="level_1")
df_temp_agg = df_temp_agg.merge(df_temp_agg_10, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
df_temp_agg_15 = df_agg.groupby(['station']).rolling(15,15, center=True,on='year').mean().reset_index()
df_temp_agg_15 = df_temp_agg_15.rename(columns={'num_mo_btw_10-22C' : 'num_mo_btw_10-22C_15yr'})
df_temp_agg_15 = df_temp_agg_15.drop(columns="level_1")
df_temp_agg = df_temp_agg.merge(df_temp_agg_15, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
df_temp_agg_20 = df_agg.groupby(['station']).rolling(20,20, center=True,on='year').mean().reset_index()
df_temp_agg_20 = df_temp_agg_20.rename(columns={'num_mo_btw_10-22C' : 'num_mo_btw_10-22C_20yr'})
df_temp_agg_20 = df_temp_agg_20.drop(columns="level_1")
df_temp_agg = df_temp_agg.merge(df_temp_agg_20, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
df_temp_agg_30 = df_agg.groupby(['station']).rolling(30,30, center=True,on='year').mean().reset_index()
df_temp_agg_30 = df_temp_agg_30.rename(columns={'num_mo_btw_10-22C' : 'num_mo_btw_10-22C_30yr'})
df_temp_agg_30 = df_temp_agg_30.drop(columns="level_1")
df_temp_agg = df_temp_agg.merge(df_temp_agg_30, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
df_temp_agg = df_temp_agg.drop(columns="num_mo_btw_10-22C")
df_agg = df_agg.merge(df_temp_agg, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
df_temp_agg = df_temp.groupby(['station', 'year'])[['value']].min().reset_index()
df_temp_agg = df_temp_agg.rename(columns={'value': 'temp_coldest_mo'})
df_agg = df_agg.merge(df_temp_agg, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
df_temp_agg_5 = df_temp_agg.groupby(['station']).rolling(5,5, center=True,on='year').mean().reset_index()
df_temp_agg_5 = df_temp_agg_5.rename(columns={'temp_coldest_mo' : 'temp_coldest_mo_5yr'})
df_temp_agg_5 = df_temp_agg_5.drop(columns="level_1")
df_agg = df_agg.merge(df_temp_agg_5, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
df_temp_agg_10 = df_temp_agg.groupby(['station']).rolling(10,10, center=True,on='year').mean().reset_index()
df_temp_agg_10 = df_temp_agg_10.rename(columns={'temp_coldest_mo' : 'temp_coldest_mo_10yr'})
df_temp_agg_10 = df_temp_agg_10.drop(columns="level_1")
df_agg = df_agg.merge(df_temp_agg_10, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
df_temp_agg_15 = df_temp_agg.groupby(['station']).rolling(15,15, center=True,on='year').mean().reset_index()
df_temp_agg_15 = df_temp_agg_15.rename(columns={'temp_coldest_mo' : 'temp_coldest_mo_15yr'})
df_temp_agg_15 = df_temp_agg_15.drop(columns="level_1")
df_agg = df_agg.merge(df_temp_agg_15, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
df_temp_agg_20 = df_temp_agg.groupby(['station']).rolling(20,20, center=True,on='year').mean().reset_index()
df_temp_agg_20 = df_temp_agg_20.rename(columns={'temp_coldest_mo' : 'temp_coldest_mo_20yr'})
df_temp_agg_20 = df_temp_agg_20.drop(columns="level_1")
df_agg = df_agg.merge(df_temp_agg_20, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
df_temp_agg_30 = df_temp_agg.groupby(['station']).rolling(30,30, center=True,on='year').mean().reset_index()
df_temp_agg_30 = df_temp_agg_30.rename(columns={'temp_coldest_mo' : 'temp_coldest_mo_30yr'})
df_temp_agg_30 = df_temp_agg_30.drop(columns="level_1")
df_agg = df_agg.merge(df_temp_agg_30, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
df_temp_agg = df_temp.groupby(['station', 'year'])[['value']].max().reset_index()
df_temp_agg = df_temp_agg.rename(columns={'value': 'temp_hottest_mo'})
df_agg = df_agg.merge(df_temp_agg, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
df_temp_agg_5 = df_temp_agg.groupby(['station']).rolling(5,5, center=True,on='year').mean().reset_index()
df_temp_agg_5 = df_temp_agg_5.rename(columns={'temp_hottest_mo' : 'temp_hottest_mo_5yr'})
df_temp_agg_5 = df_temp_agg_5.drop(columns="level_1")
df_agg = df_agg.merge(df_temp_agg_5, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
df_temp_agg_10 = df_temp_agg.groupby(['station']).rolling(10,10, center=True,on='year').mean().reset_index()
df_temp_agg_10 = df_temp_agg_10.rename(columns={'temp_hottest_mo' : 'temp_hottest_mo_10yr'})
df_temp_agg_10 = df_temp_agg_10.drop(columns="level_1")
df_agg = df_agg.merge(df_temp_agg_10, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
df_temp_agg_15 = df_temp_agg.groupby(['station']).rolling(15,15, center=True,on='year').mean().reset_index()
df_temp_agg_15 = df_temp_agg_15.rename(columns={'temp_hottest_mo' : 'temp_hottest_mo_15yr'})
df_temp_agg_15 = df_temp_agg_15.drop(columns="level_1")
df_agg = df_agg.merge(df_temp_agg_15, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
df_temp_agg_20 = df_temp_agg.groupby(['station']).rolling(20,20, center=True,on='year').mean().reset_index()
df_temp_agg_20 = df_temp_agg_20.rename(columns={'temp_hottest_mo' : 'temp_hottest_mo_20yr'})
df_temp_agg_20 = df_temp_agg_20.drop(columns="level_1")
df_agg = df_agg.merge(df_temp_agg_20, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
df_temp_agg_30 = df_temp_agg.groupby(['station']).rolling(30,30, center=True,on='year').mean().reset_index()
df_temp_agg_30 = df_temp_agg_30.rename(columns={'temp_hottest_mo' : 'temp_hottest_mo_30yr'})
df_temp_agg_30 = df_temp_agg_30.drop(columns="level_1")
df_agg = df_agg.merge(df_temp_agg_30, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
df_temp_agg = df_temp.groupby(['station', 'year'])[['value']].mean().reset_index()
df_temp_agg = df_temp_agg.rename(columns={'value': 'avg_annual_temp(t)'})
df_agg = df_agg.merge(df_temp_agg, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
df_temp_agg_5 = df_temp_agg.groupby(['station']).rolling(5,5, center=True,on='year').mean().reset_index()
df_temp_agg_5 = df_temp_agg_5.rename(columns={'avg_annual_temp(t)' : 'avg_annual_temp(t)_5yr'})
df_temp_agg_5 = df_temp_agg_5.drop(columns="level_1")
df_agg = df_agg.merge(df_temp_agg_5, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
df_temp_agg_10 = df_temp_agg.groupby(['station']).rolling(10,10, center=True,on='year').mean().reset_index()
df_temp_agg_10 = df_temp_agg_10.rename(columns={'avg_annual_temp(t)' : 'avg_annual_temp(t)_10yr'})
df_temp_agg_10 = df_temp_agg_10.drop(columns="level_1")
df_agg = df_agg.merge(df_temp_agg_10, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
df_temp_agg_15 = df_temp_agg.groupby(['station']).rolling(15,15, center=True,on='year').mean().reset_index()
df_temp_agg_15 = df_temp_agg_15.rename(columns={'avg_annual_temp(t)' : 'avg_annual_temp(t)_15yr'})
df_temp_agg_15 = df_temp_agg_15.drop(columns="level_1")
df_agg = df_agg.merge(df_temp_agg_15, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
df_temp_agg_20 = df_temp_agg.groupby(['station']).rolling(20,20, center=True,on='year').mean().reset_index()
df_temp_agg_20 = df_temp_agg_20.rename(columns={'avg_annual_temp(t)' : 'avg_annual_temp(t)_20yr'})
df_temp_agg_20 = df_temp_agg_20.drop(columns="level_1")
df_agg = df_agg.merge(df_temp_agg_20, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
df_temp_agg_30 = df_temp_agg.groupby(['station']).rolling(30,30, center=True,on='year').mean().reset_index()
df_temp_agg_30 = df_temp_agg_30.rename(columns={'avg_annual_temp(t)' : 'avg_annual_temp(t)_30yr'})
df_temp_agg_30 = df_temp_agg_30.drop(columns="level_1")
df_agg = df_agg.merge(df_temp_agg_30, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
del_list = [df_temp, df_temp_agg, df_temp_agg_5, df_temp_agg_10,df_temp_agg_15,df_temp_agg_20,df_temp_agg_30]
del del_list
del df_temp
del df_temp_agg

# calculate precipitation metrics per station per year
df_prcp = df[(df['datatype'] == 'PRCP')]
df_prcp = df_prcp[['date', 'year', 'datatype', 'station', 'value', 'year_half']]
df_prcp_agg = df_prcp.groupby(['station', 'year'])[['value']].min().reset_index()
df_prcp_agg = df_prcp_agg.rename(columns={'value': 'prcp_driest_mo'})
df_agg = df_agg.merge(df_prcp_agg, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
df_prcp_agg_5 = df_prcp_agg.groupby(['station']).rolling(5,5, center=True,on='year').mean().reset_index()
df_prcp_agg_5 = df_prcp_agg_5.rename(columns={'prcp_driest_mo' : 'prcp_driest_mo_5yr'})
df_prcp_agg_5 = df_prcp_agg_5.drop(columns="level_1")
df_agg = df_agg.merge(df_prcp_agg_5, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
df_prcp_agg_10 = df_prcp_agg.groupby(['station']).rolling(10,10, center=True,on='year').mean().reset_index()
df_prcp_agg_10 = df_prcp_agg_10.rename(columns={'prcp_driest_mo' : 'prcp_driest_mo_10yr'})
df_prcp_agg_10 = df_prcp_agg_10.drop(columns="level_1")
df_agg = df_agg.merge(df_prcp_agg_10, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
df_prcp_agg_15 = df_prcp_agg.groupby(['station']).rolling(15,15, center=True,on='year').mean().reset_index()
df_prcp_agg_15 = df_prcp_agg_15.rename(columns={'prcp_driest_mo' : 'prcp_driest_mo_15yr'})
df_prcp_agg_15 = df_prcp_agg_15.drop(columns="level_1")
df_agg = df_agg.merge(df_prcp_agg_15, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
df_prcp_agg_20 = df_prcp_agg.groupby(['station']).rolling(20,20, center=True,on='year').mean().reset_index()
df_prcp_agg_20 = df_prcp_agg_20.rename(columns={'prcp_driest_mo' : 'prcp_driest_mo_20yr'})
df_prcp_agg_20 = df_prcp_agg_20.drop(columns="level_1")
df_agg = df_agg.merge(df_prcp_agg_20, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
df_prcp_agg_30 = df_prcp_agg.groupby(['station']).rolling(30,30, center=True,on='year').mean().reset_index()
df_prcp_agg_30 = df_prcp_agg_30.rename(columns={'prcp_driest_mo' : 'prcp_driest_mo_30yr'})
df_prcp_agg_30 = df_prcp_agg_30.drop(columns="level_1")
df_agg = df_agg.merge(df_prcp_agg_30, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
df_prcp_agg = df_prcp.groupby(['station', 'year'])[['value']].max().reset_index()
df_prcp_agg = df_prcp_agg.rename(columns={'value': 'prcp_wettest_mo'})
df_agg = df_agg.merge(df_prcp_agg, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
df_prcp_agg_5 = df_prcp_agg.groupby(['station']).rolling(5,5, center=True,on='year').mean().reset_index()
df_prcp_agg_5 = df_prcp_agg_5.rename(columns={'prcp_wettest_mo' : 'prcp_wettest_mo_5yr'})
df_prcp_agg_5 = df_prcp_agg_5.drop(columns="level_1")
df_agg = df_agg.merge(df_prcp_agg_5, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
df_prcp_agg_10 = df_prcp_agg.groupby(['station']).rolling(10,10, center=True,on='year').mean().reset_index()
df_prcp_agg_10 = df_prcp_agg_10.rename(columns={'prcp_wettest_mo' : 'prcp_wettest_mo_10yr'})
df_prcp_agg_10 = df_prcp_agg_10.drop(columns="level_1")
df_agg = df_agg.merge(df_prcp_agg_10, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
df_prcp_agg_15 = df_prcp_agg.groupby(['station']).rolling(15,15, center=True,on='year').mean().reset_index()
df_prcp_agg_15 = df_prcp_agg_15.rename(columns={'prcp_wettest_mo' : 'prcp_wettest_mo_15yr'})
df_prcp_agg_15 = df_prcp_agg_15.drop(columns="level_1")
df_agg = df_agg.merge(df_prcp_agg_15, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
df_prcp_agg_20 = df_prcp_agg.groupby(['station']).rolling(20,20, center=True,on='year').mean().reset_index()
df_prcp_agg_20 = df_prcp_agg_20.rename(columns={'prcp_wettest_mo' : 'prcp_wettest_mo_20yr'})
df_prcp_agg_20 = df_prcp_agg_20.drop(columns="level_1")
df_agg = df_agg.merge(df_prcp_agg_20, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
df_prcp_agg_30 = df_prcp_agg.groupby(['station']).rolling(30,30, center=True,on='year').mean().reset_index()
df_prcp_agg_30 = df_prcp_agg_30.rename(columns={'prcp_wettest_mo' : 'prcp_wettest_mo_30yr'})
df_prcp_agg_30 = df_prcp_agg_30.drop(columns="level_1")
df_agg = df_agg.merge(df_prcp_agg_30, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
df_prcp_agg = df_prcp.groupby(['station', 'year'])[['value']].sum().reset_index()
df_prcp_agg = df_prcp_agg.rename(columns={'value': 'annual_prcp(r)'})
df_agg = df_agg.merge(df_prcp_agg, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
df_prcp_agg_5 = df_prcp_agg.groupby(['station']).rolling(5,5, center=True,on='year').mean().reset_index()
df_prcp_agg_5 = df_prcp_agg_5.rename(columns={'annual_prcp(r)' : 'annual_prcp(r)_5yr'})
df_prcp_agg_5 = df_prcp_agg_5.drop(columns="level_1")
df_agg = df_agg.merge(df_prcp_agg_5, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
df_prcp_agg_10 = df_prcp_agg.groupby(['station']).rolling(10,10, center=True,on='year').mean().reset_index()
df_prcp_agg_10 = df_prcp_agg_10.rename(columns={'annual_prcp(r)' : 'annual_prcp(r)_10yr'})
df_prcp_agg_10 = df_prcp_agg_10.drop(columns="level_1")
df_agg = df_agg.merge(df_prcp_agg_10, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
df_prcp_agg_15 = df_prcp_agg.groupby(['station']).rolling(15,15, center=True,on='year').mean().reset_index()
df_prcp_agg_15 = df_prcp_agg_15.rename(columns={'annual_prcp(r)' : 'annual_prcp(r)_15yr'})
df_prcp_agg_15 = df_prcp_agg_15.drop(columns="level_1")
df_agg = df_agg.merge(df_prcp_agg_15, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
df_prcp_agg_20 = df_prcp_agg.groupby(['station']).rolling(20,20, center=True,on='year').mean().reset_index()
df_prcp_agg_20 = df_prcp_agg_20.rename(columns={'annual_prcp(r)' : 'annual_prcp(r)_20yr'})
df_prcp_agg_20 = df_prcp_agg_20.drop(columns="level_1")
df_agg = df_agg.merge(df_prcp_agg_20, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
df_prcp_agg_30 = df_prcp_agg.groupby(['station']).rolling(30,30, center=True,on='year').mean().reset_index()
df_prcp_agg_30 = df_prcp_agg_30.rename(columns={'annual_prcp(r)' : 'annual_prcp(r)_30yr'})
df_prcp_agg_30 = df_prcp_agg_30.drop(columns="level_1")
df_agg = df_agg.merge(df_prcp_agg_30, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
df_prcp_summer = df_prcp[(df_prcp['year_half'] == 'summer')]
df_prcp_summer_agg = df_prcp_summer.groupby(['station', 'year'])[['value']].sum().reset_index()
df_prcp_summer_agg = df_prcp_summer_agg.rename(columns={'value': 'prcp_summer'})
df_agg = df_agg.merge(df_prcp_summer_agg, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
df_prcp_agg_5 = df_prcp_summer_agg.groupby(['station']).rolling(5,5, center=True,on='year').mean().reset_index()
df_prcp_agg_5 = df_prcp_agg_5.rename(columns={'prcp_summer' : 'prcp_summer_5yr'})
df_prcp_agg_5 = df_prcp_agg_5.drop(columns="level_1")
df_agg = df_agg.merge(df_prcp_agg_5, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
df_prcp_agg_10 = df_prcp_summer_agg.groupby(['station']).rolling(10,10, center=True,on='year').mean().reset_index()
df_prcp_agg_10 = df_prcp_agg_10.rename(columns={'prcp_summer' : 'prcp_summer_10yr'})
df_prcp_agg_10 = df_prcp_agg_10.drop(columns="level_1")
df_agg = df_agg.merge(df_prcp_agg_10, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
df_prcp_agg_15 = df_prcp_summer_agg.groupby(['station']).rolling(15,15, center=True,on='year').mean().reset_index()
df_prcp_agg_15 = df_prcp_agg_15.rename(columns={'prcp_summer' : 'prcp_summer_15yr'})
df_prcp_agg_15 = df_prcp_agg_15.drop(columns="level_1")
df_agg = df_agg.merge(df_prcp_agg_15, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
df_prcp_agg_20 = df_prcp_summer_agg.groupby(['station']).rolling(20,20, center=True,on='year').mean().reset_index()
df_prcp_agg_20 = df_prcp_agg_20.rename(columns={'prcp_summer' : 'prcp_summer_20yr'})
df_prcp_agg_20 = df_prcp_agg_20.drop(columns="level_1")
df_agg = df_agg.merge(df_prcp_agg_20, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
df_prcp_agg_30 = df_prcp_summer_agg.groupby(['station']).rolling(30,30, center=True,on='year').mean().reset_index()
df_prcp_agg_30 = df_prcp_agg_30.rename(columns={'prcp_summer' : 'prcp_summer_30yr'})
df_prcp_agg_30 = df_prcp_agg_30.drop(columns="level_1")
df_agg = df_agg.merge(df_prcp_agg_30, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
df_prcp_summer_agg = df_prcp_summer.groupby(['station', 'year'])[['value']].min().reset_index()
df_prcp_summer_agg = df_prcp_summer_agg.rename(columns={'value': 'prcp_driest_mo_summer'})
df_agg = df_agg.merge(df_prcp_summer_agg, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
df_prcp_agg_5 = df_prcp_summer_agg.groupby(['station']).rolling(5,5, center=True,on='year').mean().reset_index()
df_prcp_agg_5 = df_prcp_agg_5.rename(columns={'prcp_driest_mo_summer' : 'prcp_driest_mo_summer_5yr'})
df_prcp_agg_5 = df_prcp_agg_5.drop(columns="level_1")
df_agg = df_agg.merge(df_prcp_agg_5, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
df_prcp_agg_10 = df_prcp_summer_agg.groupby(['station']).rolling(10,10, center=True,on='year').mean().reset_index()
df_prcp_agg_10 = df_prcp_agg_10.rename(columns={'prcp_driest_mo_summer' : 'prcp_driest_mo_summer_10yr'})
df_prcp_agg_10 = df_prcp_agg_10.drop(columns="level_1")
df_agg = df_agg.merge(df_prcp_agg_10, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
df_prcp_agg_15 = df_prcp_summer_agg.groupby(['station']).rolling(15,15, center=True,on='year').mean().reset_index()
df_prcp_agg_15 = df_prcp_agg_15.rename(columns={'prcp_driest_mo_summer' : 'prcp_driest_mo_summer_15yr'})
df_prcp_agg_15 = df_prcp_agg_15.drop(columns="level_1")
df_agg = df_agg.merge(df_prcp_agg_15, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
df_prcp_agg_20 = df_prcp_summer_agg.groupby(['station']).rolling(20,20, center=True,on='year').mean().reset_index()
df_prcp_agg_20 = df_prcp_agg_20.rename(columns={'prcp_driest_mo_summer' : 'prcp_driest_mo_summer_20yr'})
df_prcp_agg_20 = df_prcp_agg_20.drop(columns="level_1")
df_agg = df_agg.merge(df_prcp_agg_20, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
df_prcp_agg_30 = df_prcp_summer_agg.groupby(['station']).rolling(30,30, center=True,on='year').mean().reset_index()
df_prcp_agg_30 = df_prcp_agg_30.rename(columns={'prcp_driest_mo_summer' : 'prcp_driest_mo_summer_30yr'})
df_prcp_agg_30 = df_prcp_agg_30.drop(columns="level_1")
df_agg = df_agg.merge(df_prcp_agg_30, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
df_prcp_summer_agg = df_prcp_summer.groupby(['station', 'year'])[['value']].max().reset_index()
df_prcp_summer_agg = df_prcp_summer_agg.rename(columns={'value': 'prcp_wettest_mo_summer'})
df_agg = df_agg.merge(df_prcp_summer_agg, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
df_prcp_agg_5 = df_prcp_summer_agg.groupby(['station']).rolling(5,5, center=True,on='year').mean().reset_index()
df_prcp_agg_5 = df_prcp_agg_5.rename(columns={'prcp_wettest_mo_summer' : 'prcp_wettest_mo_summer_5yr'})
df_prcp_agg_5 = df_prcp_agg_5.drop(columns="level_1")
df_agg = df_agg.merge(df_prcp_agg_5, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
df_prcp_agg_10 = df_prcp_summer_agg.groupby(['station']).rolling(10,10, center=True,on='year').mean().reset_index()
df_prcp_agg_10 = df_prcp_agg_10.rename(columns={'prcp_wettest_mo_summer' : 'prcp_wettest_mo_summer_10yr'})
df_prcp_agg_10 = df_prcp_agg_10.drop(columns="level_1")
df_agg = df_agg.merge(df_prcp_agg_10, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
df_prcp_agg_15 = df_prcp_summer_agg.groupby(['station']).rolling(15,15, center=True,on='year').mean().reset_index()
df_prcp_agg_15 = df_prcp_agg_15.rename(columns={'prcp_wettest_mo_summer' : 'prcp_wettest_mo_summer_15yr'})
df_prcp_agg_15 = df_prcp_agg_15.drop(columns="level_1")
df_agg = df_agg.merge(df_prcp_agg_15, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
df_prcp_agg_20 = df_prcp_summer_agg.groupby(['station']).rolling(20,20, center=True,on='year').mean().reset_index()
df_prcp_agg_20 = df_prcp_agg_20.rename(columns={'prcp_wettest_mo_summer' : 'prcp_wettest_mo_summer_20yr'})
df_prcp_agg_20 = df_prcp_agg_20.drop(columns="level_1")
df_agg = df_agg.merge(df_prcp_agg_20, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
df_prcp_agg_30 = df_prcp_summer_agg.groupby(['station']).rolling(30,30, center=True,on='year').mean().reset_index()
df_prcp_agg_30 = df_prcp_agg_30.rename(columns={'prcp_wettest_mo_summer' : 'prcp_wettest_mo_summer_30yr'})
df_prcp_agg_30 = df_prcp_agg_30.drop(columns="level_1")
df_agg = df_agg.merge(df_prcp_agg_30, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
df_prcp_winter = df_prcp[(df_prcp['year_half'] == 'winter')]
df_prcp_winter_agg = df_prcp_winter.groupby(['station', 'year'])[['value']].sum().reset_index()
df_prcp_winter_agg = df_prcp_winter_agg.rename(columns={'value': 'prcp_winter'})
df_agg = df_agg.merge(df_prcp_winter_agg, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
df_prcp_agg_5 = df_prcp_winter_agg.groupby(['station']).rolling(5,5, center=True,on='year').mean().reset_index()
df_prcp_agg_5 = df_prcp_agg_5.rename(columns={'prcp_winter' : 'prcp_winter_5yr'})
df_prcp_agg_5 = df_prcp_agg_5.drop(columns="level_1")
df_agg = df_agg.merge(df_prcp_agg_5, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
df_prcp_agg_10 = df_prcp_winter_agg.groupby(['station']).rolling(10,10, center=True,on='year').mean().reset_index()
df_prcp_agg_10 = df_prcp_agg_10.rename(columns={'prcp_winter' : 'prcp_winter_10yr'})
df_prcp_agg_10 = df_prcp_agg_10.drop(columns="level_1")
df_agg = df_agg.merge(df_prcp_agg_10, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
df_prcp_agg_15 = df_prcp_winter_agg.groupby(['station']).rolling(15,15, center=True,on='year').mean().reset_index()
df_prcp_agg_15 = df_prcp_agg_15.rename(columns={'prcp_winter' : 'prcp_winter_15yr'})
df_prcp_agg_15 = df_prcp_agg_15.drop(columns="level_1")
df_agg = df_agg.merge(df_prcp_agg_15, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
df_prcp_agg_20 = df_prcp_winter_agg.groupby(['station']).rolling(20,20, center=True,on='year').mean().reset_index()
df_prcp_agg_20 = df_prcp_agg_20.rename(columns={'prcp_winter' : 'prcp_winter_20yr'})
df_prcp_agg_20 = df_prcp_agg_20.drop(columns="level_1")
df_agg = df_agg.merge(df_prcp_agg_20, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
df_prcp_agg_30 = df_prcp_winter_agg.groupby(['station']).rolling(30,30, center=True,on='year').mean().reset_index()
df_prcp_agg_30 = df_prcp_agg_30.rename(columns={'prcp_winter' : 'prcp_winter_30yr'})
df_prcp_agg_30 = df_prcp_agg_30.drop(columns="level_1")
df_agg = df_agg.merge(df_prcp_agg_30, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
df_prcp_winter_agg = df_prcp_winter.groupby(['station', 'year'])[['value']].min().reset_index()
df_prcp_winter_agg = df_prcp_winter_agg.rename(columns={'value': 'prcp_driest_mo_winter'})
df_agg = df_agg.merge(df_prcp_winter_agg, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
df_prcp_agg_5 = df_prcp_winter_agg.groupby(['station']).rolling(5, 5, center=True, on='year').mean().reset_index()
df_prcp_agg_5 = df_prcp_agg_5.rename(columns={'prcp_driest_mo_winter': 'prcp_driest_mo_winter_5yr'})
df_prcp_agg_5 = df_prcp_agg_5.drop(columns="level_1")
df_agg = df_agg.merge(df_prcp_agg_5, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
df_prcp_agg_10 = df_prcp_winter_agg.groupby(['station']).rolling(10, 10, center=True, on='year').mean().reset_index()
df_prcp_agg_10 = df_prcp_agg_10.rename(columns={'prcp_driest_mo_winter': 'prcp_driest_mo_winter_10yr'})
df_prcp_agg_10 = df_prcp_agg_10.drop(columns="level_1")
df_agg = df_agg.merge(df_prcp_agg_10, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
df_prcp_agg_15 = df_prcp_winter_agg.groupby(['station']).rolling(15, 15, center=True, on='year').mean().reset_index()
df_prcp_agg_15 = df_prcp_agg_15.rename(columns={'prcp_driest_mo_winter': 'prcp_driest_mo_winter_15yr'})
df_prcp_agg_15 = df_prcp_agg_15.drop(columns="level_1")
df_agg = df_agg.merge(df_prcp_agg_15, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
df_prcp_agg_20 = df_prcp_winter_agg.groupby(['station']).rolling(20, 20, center=True, on='year').mean().reset_index()
df_prcp_agg_20 = df_prcp_agg_20.rename(columns={'prcp_driest_mo_winter': 'prcp_driest_mo_winter_20yr'})
df_prcp_agg_20 = df_prcp_agg_20.drop(columns="level_1")
df_agg = df_agg.merge(df_prcp_agg_20, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
df_prcp_agg_30 = df_prcp_winter_agg.groupby(['station']).rolling(30, 30, center=True, on='year').mean().reset_index()
df_prcp_agg_30 = df_prcp_agg_30.rename(columns={'prcp_driest_mo_winter': 'prcp_driest_mo_winter_30yr'})
df_prcp_agg_30 = df_prcp_agg_30.drop(columns="level_1")
df_agg = df_agg.merge(df_prcp_agg_30, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
df_prcp_winter_agg = df_prcp_winter.groupby(['station', 'year'])[['value']].max().reset_index()
df_prcp_winter_agg = df_prcp_winter_agg.rename(columns={'value': 'prcp_wettest_mo_winter'})
df_agg = df_agg.merge(df_prcp_winter_agg, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
df_prcp_agg_5 = df_prcp_winter_agg.groupby(['station']).rolling(5, 5, center=True, on='year').mean().reset_index()
df_prcp_agg_5 = df_prcp_agg_5.rename(columns={'prcp_wettest_mo_winter': 'prcp_wettest_mo_winter_5yr'})
df_prcp_agg_5 = df_prcp_agg_5.drop(columns="level_1")
df_agg = df_agg.merge(df_prcp_agg_5, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
df_prcp_agg_10 = df_prcp_winter_agg.groupby(['station']).rolling(10, 10, center=True, on='year').mean().reset_index()
df_prcp_agg_10 = df_prcp_agg_10.rename(columns={'prcp_wettest_mo_winter': 'prcp_wettest_mo_winter_10yr'})
df_prcp_agg_10 = df_prcp_agg_10.drop(columns="level_1")
df_agg = df_agg.merge(df_prcp_agg_10, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
df_prcp_agg_15 = df_prcp_winter_agg.groupby(['station']).rolling(15, 15, center=True, on='year').mean().reset_index()
df_prcp_agg_15 = df_prcp_agg_15.rename(columns={'prcp_wettest_mo_winter': 'prcp_wettest_mo_winter_15yr'})
df_prcp_agg_15 = df_prcp_agg_15.drop(columns="level_1")
df_agg = df_agg.merge(df_prcp_agg_15, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
df_prcp_agg_20 = df_prcp_winter_agg.groupby(['station']).rolling(20, 20, center=True, on='year').mean().reset_index()
df_prcp_agg_20 = df_prcp_agg_20.rename(columns={'prcp_wettest_mo_winter': 'prcp_wettest_mo_winter_20yr'})
df_prcp_agg_20 = df_prcp_agg_20.drop(columns="level_1")
df_agg = df_agg.merge(df_prcp_agg_20, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
df_prcp_agg_30 = df_prcp_winter_agg.groupby(['station']).rolling(30, 30, center=True, on='year').mean().reset_index()
df_prcp_agg_30 = df_prcp_agg_30.rename(columns={'prcp_wettest_mo_winter': 'prcp_wettest_mo_winter_30yr'})
df_prcp_agg_30 = df_prcp_agg_30.drop(columns="level_1")
df_agg = df_agg.merge(df_prcp_agg_30, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
del_list = [df, df_prcp_winter_agg, df_prcp_winter, df_prcp_summer, df_prcp_summer_agg, df_prcp_agg, df_prcp, df_prcp_agg_5, df_prcp_agg_10, df_prcp_agg_15,df_prcp_agg_20,df_prcp_agg_30]
del del_list
del df
del df_prcp_winter_agg
del df_prcp_winter
del df_prcp_summer
del df_prcp_summer_agg
del df_prcp_agg
del df_prcp
del df_prcp_agg_30
del df_prcp_agg_20
del df_prcp_agg_15
del df_prcp_agg_10
del df_prcp_agg_5
del df_temp_agg_5
del df_temp_agg_10
del df_temp_agg_15
del df_temp_agg_20
del df_temp_agg_30

# bring in the station location data
df_agg = df_agg.merge(df_station_list, how="left", left_on=['station'], right_on=['station'])


def koppen(s):
    # run class B first because Arid climates can intersect with the other types, but we want B to take priority over the temp rules
    if (((s['annual_prcp(r)'] < (10 * (2 * s['avg_annual_temp(t)']))) & (
            s['prcp_winter'] >= (.7 * s['annual_prcp(r)']))) | (
            (s['annual_prcp(r)'] < (10 * ((2 * s['avg_annual_temp(t)']) + 28))) & (
            s['prcp_summer'] >= (.7 * s['annual_prcp(r)']))) | (
    (s['annual_prcp(r)'] < (10 * ((2 * s['avg_annual_temp(t)']) + 14))))):
        if (((s['annual_prcp(r)'] < (5 * (2 * s['avg_annual_temp(t)']))) & (
                s['prcp_winter'] >= (.7 * s['annual_prcp(r)']))) | (
                (s['annual_prcp(r)'] < (5 * ((2 * s['avg_annual_temp(t)']) + 28))) & (
                s['prcp_summer'] >= (.7 * s['annual_prcp(r)']))) | (
        (s['annual_prcp(r)'] < (5 * ((2 * s['avg_annual_temp(t)']) + 14))))):
            if (s['avg_annual_temp(t)'] >= 18):
                return 'BWh'
            if (s['avg_annual_temp(t)'] < 18):
                return 'BWk'
        if (((s['annual_prcp(r)'] >= (5 * (2 * s['avg_annual_temp(t)']))) & (
                s['prcp_winter'] >= (.7 * s['annual_prcp(r)']))) | (
                (s['annual_prcp(r)'] >= (5 * ((2 * s['avg_annual_temp(t)']) + 28))) & (
                s['prcp_summer'] >= (.7 * s['annual_prcp(r)']))) | (
        (s['annual_prcp(r)'] >= (5 * ((2 * s['avg_annual_temp(t)']) + 14))))):
            if (s['avg_annual_temp(t)'] >= 18):
                return 'BSh'
            if (s['avg_annual_temp(t)'] < 18):
                return 'BSk'
    elif (s['temp_coldest_mo'] >= 18):
        if (s['prcp_driest_mo'] >= 60):
            return 'Af'
        if ((s['prcp_driest_mo'] < 60) & (s['prcp_driest_mo'] >= (100 - (s['annual_prcp(r)'] / 25)))):
            return 'Am'
        if ((s['prcp_driest_mo'] < 60) & (s['prcp_driest_mo'] < (100 - (s['annual_prcp(r)'] / 25)))):
            return 'Aw'
    elif ((s['temp_coldest_mo'] > 0) & (s['temp_coldest_mo'] < 18)):
        if ((s['prcp_driest_mo_summer'] < 40) & (s['prcp_driest_mo_summer'] < (s['prcp_wettest_mo_winter'] / 3))):
            if (s['temp_hottest_mo'] >= 22):
                return 'Csa'
            elif (s['num_mo_btw_10-22C'] >= 4):
                return 'Csb'
            elif (s['num_mo_btw_10-22C'] < 4):
                return 'Csc'
            else:
                return 'Cs'
        elif (s['prcp_driest_mo_winter'] < (s['prcp_wettest_mo_summer'] / 10)):
            if (s['temp_hottest_mo'] >= 22):
                return 'Cwa'
            elif (s['num_mo_btw_10-22C'] >= 4):
                return 'Cwb'
            elif (s['num_mo_btw_10-22C'] < 4):
                return 'Cwc'
            else:
                return 'Cw'
        else:
            if (s['temp_hottest_mo'] >= 22):
                return 'Cfa'
            elif (s['num_mo_btw_10-22C'] >= 4):
                return 'Cfb'
            elif (s['num_mo_btw_10-22C'] < 4):
                return 'Cfc'
            else:
                return 'Cf'
    elif ((s['temp_coldest_mo'] <= 0) & (s['temp_hottest_mo'] > 10)):
        if ((s['prcp_driest_mo_summer'] < 40) & (s['prcp_driest_mo_summer'] < (s['prcp_wettest_mo_winter'] / 3))):
            if (s['temp_hottest_mo'] >= 22):
                return 'Dsa'
            elif (s['num_mo_btw_10-22C'] >= 4):
                return 'Dsb'
            elif (s['temp_coldest_mo'] < -38):
                return 'Dsd'
            else:
                return 'Dsc'
        elif (s['prcp_driest_mo_winter'] < (s['prcp_wettest_mo_summer'] / 10)):
            if (s['temp_hottest_mo'] >= 22):
                return 'Dwa'
            elif (s['num_mo_btw_10-22C'] >= 4):
                return 'Dwb'
            elif (s['temp_coldest_mo'] < -38):
                return 'Dwd'
            else:
                return 'Dwc'
        else:
            if (s['temp_hottest_mo'] >= 22):
                return 'Dfa'
            elif (s['num_mo_btw_10-22C'] >= 4):
                return 'Dfb'
            elif (s['temp_coldest_mo'] < -38):
                return 'Dfd'
            else:
                return 'Dfc'
    elif (s['temp_hottest_mo'] <= 10):
        if (s['temp_hottest_mo'] > 0):
            return 'ET'
        if (s['temp_hottest_mo'] <= 0):
            return 'EF'
    elif (s['elevation'] > 1500):
        return 'H'


df_agg['koppen'] = df_agg.apply(koppen, axis=1)


def koppen_name(s):
    if (s['koppen'] == 'Af'):
        return 'Tropical Rainforest'
    if (s['koppen'] == 'Am'):
        return 'Tropical Monsoon'
    if (s['koppen'] == 'Aw'):
        return 'Savannah'
    elif (s['koppen'] == 'BWh'):
        return 'Hot desert'
    elif (s['koppen'] == 'BWk'):
        return 'Cold desert'
    elif (s['koppen'] == 'BSh'):
        return 'Hot steppe'
    elif (s['koppen'] == 'BSk'):
        return 'Cold steppe'
    elif (s['koppen'] == 'Csa'):
        return 'Mediterranean hot'
    elif (s['koppen'] == 'Csb'):
        return 'Mediterranean warm'
    elif (s['koppen'] == 'Csc'):
        return 'Mediterranean cold'
    elif (s['koppen'] == 'Cfa'):
        return 'Humid subtropical'
    elif (s['koppen'] == 'Cfb'):
        return 'Oceanic'
    elif (s['koppen'] == 'Cfc'):
        return 'Subpolar oceanic'
    elif (s['koppen'] == 'Cwa'):
        return 'Subtropical)'
    elif (s['koppen'] == 'Cwb'):
        return 'Subtropical highland warm'
    elif (s['koppen'] == 'Cwc'):
        return 'Subtropical highland cold'
    elif (s['koppen'] == 'Dsa'):
        return 'Continental (dry summer)'
    elif (s['koppen'] == 'Dwa'):
        return 'Continental (dry winter)'
    elif (s['koppen'] == 'Dfa'):
        return 'Continental (no dry season)'
    elif (s['koppen'] == 'Dsb'):
        return 'Hemiboreal (dry summer)'
    elif (s['koppen'] == 'Dwb'):
        return 'Hemiboreal (dry winter)'
    elif (s['koppen'] == 'Dfb'):
        return 'Hemiboreal (no dry season)'
    elif (s['koppen'] == 'Dsc'):
        return 'Boreal (dry summer)'
    elif (s['koppen'] == 'Dwc'):
        return 'Boreal (dry winter)'
    elif (s['koppen'] == 'Dfc'):
        return 'Boreal (no dry season)'
    elif (s['koppen'] == 'Dsd'):
        return 'Cold Boreal (dry summer)'
    elif (s['koppen'] == 'Dwd'):
        return 'Cold Boreal (dry winter)'
    elif (s['koppen'] == 'Dfd'):
        return 'Cold Boreal (no dry season)'
    elif (s['koppen'] == 'EF'):
        return 'Ice cap'
    elif (s['koppen'] == 'ET'):
        return 'Tundra'
    elif (s['koppen'] == 'H'):
        return 'Highland'


df_agg['koppen_name'] = df_agg.apply(koppen_name, axis=1)
print("assigning Köppen classes")

# COMMENT OUT IF ADDING TO EXISTING
# if os.path.exists('data/koppen_class.csv'):
#     os.rename('data/koppen_class.csv', 'data/koppen_class_old.csv')
#
# df_agg.to_csv(df_filename, index=False, quotechar='"', quoting=csv.QUOTE_ALL, header=True)
# print("exporting " + df_filename)

# #IF EXISTING
df_agg.to_csv(df_filename, index=False, quotechar='"', mode="a", quoting=csv.QUOTE_ALL, header=False)

# check and de-duplicate
df_check = pd.read_csv(df_filename)
df_check = df_check.drop_duplicates()

df_check.to_csv(df_filename, index=False, quotechar='"', quoting=csv.QUOTE_ALL, header=True)
print("exporting " + df_filename)
