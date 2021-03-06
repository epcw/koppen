# RUN THIS LAST - only once you have all your data pulled and in /data
# THIS SCRIPT takes data pulled from NOAA_data_pull.py and then assigns a koppen climate code to each station in the dataset for each year that data exists

import pandas as pd
import csv
import os

df_filename = 'data/koppen_class.csv'

df = pd.read_csv('data/station_data.csv')
df['date'] = pd.to_datetime(df['date'])  # converts the date string to datetime
# df['date'] = df['date'].dt.date #drops the midnight timestamp - comment out if this is actually meaningful for you

df_station_list = pd.read_csv('stations/station_list_complete.csv')
df_station_list = df_station_list[['elevation', 'mindate', 'maxdate', 'latitude', 'longitude', 'name', 'id']]
df_station_list = df_station_list.rename(columns={'id': 'station'})
df_station_list['elevation'] = pd.to_numeric(df_station_list.elevation, errors='coerce') #this accounts for some wacko cells in the elevation column - probably string NaNs or "N/A" or "unknown" or some other manual notes written in grease pen from a surveyor in like 1943.

df = df.merge(df_station_list, how='inner', left_on=['station'], right_on=['station'])

winter_months = [1, 2, 3, 10, 11, 12]  # from Koppen climate classification for N hemisphere
df['year_half'] = 'summer'
df.loc[df['date'].dt.month.isin(winter_months), 'year_half'] = 'winter'

df['year'] = df['date'].dt.year
years_to_avg = [5, 10, 15, 20, 30] # don't bother to include the 1 since you don't have to calc this as a rolling function POSSIBLY NAME TIMESCALE FOR PEOPLE WHO AREN'T ME AND RICH TO KNOW WHAT THE FUCK THIS IS

# calculate temperature metrics per station per year
df_temp = df[(df['datatype'] == 'TAVG')]
df_temp = df_temp[['date', 'year', 'datatype', 'station', 'value']]
df_temp['num_mo_btw_10-22C'] = 0
df_temp.loc[(df['value'] > 10) & (df_temp['value'] < 22), 'num_mo_btw_10-22C'] = 1
df_agg = df_temp.groupby(['station', 'year'])[['num_mo_btw_10-22C']].sum().reset_index()
df_agg['years_averaged'] = 1
df_agg['num_mo_btw_10-22C'] = df_agg['num_mo_btw_10-22C'].astype('float64')
df_agg['year'] = df_agg['year'].astype('int')
df_agg['years_averaged'] = df_agg['years_averaged'].astype('int')

df_temp_agg = df_agg

for y in years_to_avg:
    df_temp_agg_temp = df_temp_agg.groupby(['station']).rolling(y,y, center=True,on='year').mean().reset_index()
    df_temp_agg_temp = df_temp_agg_temp.drop(columns="level_1")
    df_temp_agg_temp['years_averaged'] = y
    df_agg = pd.concat([df_agg,df_temp_agg_temp],axis=0).drop_duplicates()

    # df_temp_agg_10 = df_agg.groupby(['station']).rolling(10,10, center=True,on='year').mean().reset_index()
    # df_temp_agg_10 = df_temp_agg_10.rename(columns={'num_mo_btw_10-22C' : 'num_mo_btw_10-22C_10yr'})
    # df_temp_agg_10 = df_temp_agg_10.drop(columns="level_1")
    # df_temp_agg = df_temp_agg.merge(df_temp_agg_10, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
    # df_temp_agg_15 = df_agg.groupby(['station']).rolling(15,15, center=True,on='year').mean().reset_index()
    # df_temp_agg_15 = df_temp_agg_15.rename(columns={'num_mo_btw_10-22C' : 'num_mo_btw_10-22C_15yr'})
    # df_temp_agg_15 = df_temp_agg_15.drop(columns="level_1")
    # df_temp_agg = df_temp_agg.merge(df_temp_agg_15, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
    # df_temp_agg_20 = df_agg.groupby(['station']).rolling(20,20, center=True,on='year').mean().reset_index()
    # df_temp_agg_20 = df_temp_agg_20.rename(columns={'num_mo_btw_10-22C' : 'num_mo_btw_10-22C_20yr'})
    # df_temp_agg_20 = df_temp_agg_20.drop(columns="level_1")
    # df_temp_agg = df_temp_agg.merge(df_temp_agg_20, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
    # df_temp_agg_30 = df_agg.groupby(['station']).rolling(30,30, center=True,on='year').mean().reset_index()
    # df_temp_agg_30 = df_temp_agg_30.rename(columns={'num_mo_btw_10-22C' : 'num_mo_btw_10-22C_30yr'})
    # df_temp_agg_30 = df_temp_agg_30.drop(columns="level_1")
    # df_temp_agg = df_temp_agg.merge(df_temp_agg_30, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
    # df_temp_agg = df_temp_agg.drop(columns="num_mo_btw_10-22C")
    # df_agg = df_agg.merge(df_temp_agg, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])

df_temp_agg = df_temp.groupby(['station', 'year'])[['value']].min().reset_index()
df_temp_agg = df_temp_agg.rename(columns={'value': 'temp_coldest_mo'})
df_temp_agg['years_averaged'] = 1
df_coldest = df_temp_agg

for y in years_to_avg:
    df_temp_agg_temp = df_temp_agg.groupby(['station']).rolling(y,y, center=True,on='year').mean().reset_index()
    df_temp_agg_temp = df_temp_agg_temp.drop(columns="level_1")
    df_temp_agg_temp['years_averaged'] = y
    df_coldest = pd.concat([df_coldest,df_temp_agg_temp],axis=0).drop_duplicates()

df_agg = df_agg.merge(df_coldest, how="outer",  left_on=['station', 'year','years_averaged'], right_on=['station', 'year','years_averaged']).drop_duplicates()

# df_temp_agg_5 = df_temp_agg.groupby(['station']).rolling(5,5, center=True,on='year').mean().reset_index()
# df_temp_agg_5 = df_temp_agg_5.rename(columns={'temp_coldest_mo' : 'temp_coldest_mo_5yr'})
# df_temp_agg_5 = df_temp_agg_5.drop(columns="level_1")
# df_agg = df_agg.merge(df_temp_agg_5, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
# df_temp_agg_10 = df_temp_agg.groupby(['station']).rolling(10,10, center=True,on='year').mean().reset_index()
# df_temp_agg_10 = df_temp_agg_10.rename(columns={'temp_coldest_mo' : 'temp_coldest_mo_10yr'})
# df_temp_agg_10 = df_temp_agg_10.drop(columns="level_1")
# df_agg = df_agg.merge(df_temp_agg_10, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
# df_temp_agg_15 = df_temp_agg.groupby(['station']).rolling(15,15, center=True,on='year').mean().reset_index()
# df_temp_agg_15 = df_temp_agg_15.rename(columns={'temp_coldest_mo' : 'temp_coldest_mo_15yr'})
# df_temp_agg_15 = df_temp_agg_15.drop(columns="level_1")
# df_agg = df_agg.merge(df_temp_agg_15, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
# df_temp_agg_20 = df_temp_agg.groupby(['station']).rolling(20,20, center=True,on='year').mean().reset_index()
# df_temp_agg_20 = df_temp_agg_20.rename(columns={'temp_coldest_mo' : 'temp_coldest_mo_20yr'})
# df_temp_agg_20 = df_temp_agg_20.drop(columns="level_1")
# df_agg = df_agg.merge(df_temp_agg_20, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
# df_temp_agg_30 = df_temp_agg.groupby(['station']).rolling(30,30, center=True,on='year').mean().reset_index()
# df_temp_agg_30 = df_temp_agg_30.rename(columns={'temp_coldest_mo' : 'temp_coldest_mo_30yr'})
# df_temp_agg_30 = df_temp_agg_30.drop(columns="level_1")
# df_agg = df_agg.merge(df_temp_agg_30, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])

df_temp_agg = df_temp.groupby(['station', 'year'])[['value']].max().reset_index()
df_temp_agg = df_temp_agg.rename(columns={'value': 'temp_hottest_mo'})
df_temp_agg['years_averaged'] = 1
df_hottest = df_temp_agg

for y in years_to_avg:
    df_temp_agg_temp = df_temp_agg.groupby(['station']).rolling(y,y, center=True,on='year').mean().reset_index()
    df_temp_agg_temp = df_temp_agg_temp.drop(columns="level_1")
    df_temp_agg_temp['years_averaged'] = y
    df_hottest = pd.concat([df_hottest,df_temp_agg_temp],axis=0).drop_duplicates()

df_agg = df_agg.merge(df_hottest, how="outer",  left_on=['station', 'year','years_averaged'], right_on=['station', 'year','years_averaged']).drop_duplicates()

# df_temp_agg = df_temp.groupby(['station', 'year'])[['value']].max().reset_index()
# df_temp_agg = df_temp_agg.rename(columns={'value': 'temp_hottest_mo'})
# df_temp_agg['years_averaged'] = 1
# df_agg = df_agg.merge(df_temp_agg, how="outer", left_on=['station', 'year','years_averaged'], right_on=['station', 'year','years_averaged'])
#
# df_temp_agg_5 = df_temp_agg.groupby(['station']).rolling(5,5, center=True,on='year').mean().reset_index()
# df_temp_agg_5 = df_temp_agg_5.rename(columns={'temp_hottest_mo' : 'temp_hottest_mo_5yr'})
# df_temp_agg_5 = df_temp_agg_5.drop(columns="level_1")
# df_agg = df_agg.merge(df_temp_agg_5, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
# df_temp_agg_10 = df_temp_agg.groupby(['station']).rolling(10,10, center=True,on='year').mean().reset_index()
# df_temp_agg_10 = df_temp_agg_10.rename(columns={'temp_hottest_mo' : 'temp_hottest_mo_10yr'})
# df_temp_agg_10 = df_temp_agg_10.drop(columns="level_1")
# df_agg = df_agg.merge(df_temp_agg_10, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
# df_temp_agg_15 = df_temp_agg.groupby(['station']).rolling(15,15, center=True,on='year').mean().reset_index()
# df_temp_agg_15 = df_temp_agg_15.rename(columns={'temp_hottest_mo' : 'temp_hottest_mo_15yr'})
# df_temp_agg_15 = df_temp_agg_15.drop(columns="level_1")
# df_agg = df_agg.merge(df_temp_agg_15, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
# df_temp_agg_20 = df_temp_agg.groupby(['station']).rolling(20,20, center=True,on='year').mean().reset_index()
# df_temp_agg_20 = df_temp_agg_20.rename(columns={'temp_hottest_mo' : 'temp_hottest_mo_20yr'})
# df_temp_agg_20 = df_temp_agg_20.drop(columns="level_1")
# df_agg = df_agg.merge(df_temp_agg_20, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
# df_temp_agg_30 = df_temp_agg.groupby(['station']).rolling(30,30, center=True,on='year').mean().reset_index()
# df_temp_agg_30 = df_temp_agg_30.rename(columns={'temp_hottest_mo' : 'temp_hottest_mo_30yr'})
# df_temp_agg_30 = df_temp_agg_30.drop(columns="level_1")
# df_agg = df_agg.merge(df_temp_agg_30, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])

df_temp_agg = df_temp.groupby(['station', 'year'])[['value']].mean().reset_index()
df_temp_agg = df_temp_agg.rename(columns={'value': 'avg_annual_temp(t)'})
df_temp_agg['years_averaged'] = 1
df_mean = df_temp_agg

for y in years_to_avg:
    df_temp_agg_temp = df_temp_agg.groupby(['station']).rolling(y,y, center=True,on='year').mean().reset_index()
    df_temp_agg_temp = df_temp_agg_temp.drop(columns="level_1")
    df_temp_agg_temp['years_averaged'] = y
    df_mean = pd.concat([df_mean,df_temp_agg_temp],axis=0).drop_duplicates()

df_agg = df_agg.merge(df_mean, how="outer",  left_on=['station', 'year','years_averaged'], right_on=['station', 'year','years_averaged']).drop_duplicates()

# df_agg = df_agg.merge(df_temp_agg, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
# df_temp_agg_5 = df_temp_agg.groupby(['station']).rolling(5,5, center=True,on='year').mean().reset_index()
# df_temp_agg_5 = df_temp_agg_5.rename(columns={'avg_annual_temp(t)' : 'avg_annual_temp(t)_5yr'})
# df_temp_agg_5 = df_temp_agg_5.drop(columns="level_1")
# df_agg = df_agg.merge(df_temp_agg_5, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
# df_temp_agg_10 = df_temp_agg.groupby(['station']).rolling(10,10, center=True,on='year').mean().reset_index()
# df_temp_agg_10 = df_temp_agg_10.rename(columns={'avg_annual_temp(t)' : 'avg_annual_temp(t)_10yr'})
# df_temp_agg_10 = df_temp_agg_10.drop(columns="level_1")
# df_agg = df_agg.merge(df_temp_agg_10, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
# df_temp_agg_15 = df_temp_agg.groupby(['station']).rolling(15,15, center=True,on='year').mean().reset_index()
# df_temp_agg_15 = df_temp_agg_15.rename(columns={'avg_annual_temp(t)' : 'avg_annual_temp(t)_15yr'})
# df_temp_agg_15 = df_temp_agg_15.drop(columns="level_1")
# df_agg = df_agg.merge(df_temp_agg_15, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
# df_temp_agg_20 = df_temp_agg.groupby(['station']).rolling(20,20, center=True,on='year').mean().reset_index()
# df_temp_agg_20 = df_temp_agg_20.rename(columns={'avg_annual_temp(t)' : 'avg_annual_temp(t)_20yr'})
# df_temp_agg_20 = df_temp_agg_20.drop(columns="level_1")
# df_agg = df_agg.merge(df_temp_agg_20, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
# df_temp_agg_30 = df_temp_agg.groupby(['station']).rolling(30,30, center=True,on='year').mean().reset_index()
# df_temp_agg_30 = df_temp_agg_30.rename(columns={'avg_annual_temp(t)' : 'avg_annual_temp(t)_30yr'})
# df_temp_agg_30 = df_temp_agg_30.drop(columns="level_1")
# df_agg = df_agg.merge(df_temp_agg_30, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
del_list = [df_temp, df_temp_agg, df_temp_agg_temp, df_coldest,df_hottest,df_mean]
del del_list
del df_temp
del df_temp_agg
del df_temp_agg_temp
del df_coldest
del df_hottest
del df_mean

# calculate precipitation metrics per station per year
df_prcp = df[(df['datatype'] == 'PRCP')]
df_prcp = df_prcp[['date', 'year', 'datatype', 'station', 'value', 'year_half']]
df_prcp_agg = df_prcp.groupby(['station', 'year'])[['value']].min().reset_index()
df_prcp_agg = df_prcp_agg.rename(columns={'value': 'prcp_driest_mo'})
df_prcp_agg['years_averaged'] = 1
df_prcp_agg['years_averaged'] = df_prcp_agg['years_averaged'].astype('int')
df_driest = df_prcp_agg

for y in years_to_avg:
    df_prcp_agg_temp = df_prcp_agg.groupby(['station']).rolling(y,y, center=True,on='year').mean().reset_index()
    df_prcp_agg_temp = df_prcp_agg_temp.drop(columns="level_1")
    df_prcp_agg_temp['years_averaged'] = y
    df_driest = pd.concat([df_driest,df_prcp_agg_temp],axis=0).drop_duplicates()

df_agg = df_agg.merge(df_driest, how="outer",  left_on=['station', 'year','years_averaged'], right_on=['station', 'year','years_averaged']).drop_duplicates()

# df_prcp_agg_5 = df_prcp_agg.groupby(['station']).rolling(5,5, center=True,on='year').mean().reset_index()
# df_prcp_agg_5 = df_prcp_agg_5.rename(columns={'prcp_driest_mo' : 'prcp_driest_mo_5yr'})
# df_prcp_agg_5 = df_prcp_agg_5.drop(columns="level_1")
# df_agg = df_agg.merge(df_prcp_agg_5, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
# df_prcp_agg_10 = df_prcp_agg.groupby(['station']).rolling(10,10, center=True,on='year').mean().reset_index()
# df_prcp_agg_10 = df_prcp_agg_10.rename(columns={'prcp_driest_mo' : 'prcp_driest_mo_10yr'})
# df_prcp_agg_10 = df_prcp_agg_10.drop(columns="level_1")
# df_agg = df_agg.merge(df_prcp_agg_10, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
# df_prcp_agg_15 = df_prcp_agg.groupby(['station']).rolling(15,15, center=True,on='year').mean().reset_index()
# df_prcp_agg_15 = df_prcp_agg_15.rename(columns={'prcp_driest_mo' : 'prcp_driest_mo_15yr'})
# df_prcp_agg_15 = df_prcp_agg_15.drop(columns="level_1")
# df_agg = df_agg.merge(df_prcp_agg_15, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
# df_prcp_agg_20 = df_prcp_agg.groupby(['station']).rolling(20,20, center=True,on='year').mean().reset_index()
# df_prcp_agg_20 = df_prcp_agg_20.rename(columns={'prcp_driest_mo' : 'prcp_driest_mo_20yr'})
# df_prcp_agg_20 = df_prcp_agg_20.drop(columns="level_1")
# df_agg = df_agg.merge(df_prcp_agg_20, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
# df_prcp_agg_30 = df_prcp_agg.groupby(['station']).rolling(30,30, center=True,on='year').mean().reset_index()
# df_prcp_agg_30 = df_prcp_agg_30.rename(columns={'prcp_driest_mo' : 'prcp_driest_mo_30yr'})
# df_prcp_agg_30 = df_prcp_agg_30.drop(columns="level_1")
# df_agg = df_agg.merge(df_prcp_agg_30, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])

df_prcp_agg = df_prcp.groupby(['station', 'year'])[['value']].max().reset_index()
df_prcp_agg = df_prcp_agg.rename(columns={'value': 'prcp_wettest_mo'})
df_prcp_agg['years_averaged'] = 1
df_prcp_agg['years_averaged'] = df_prcp_agg['years_averaged'].astype('int')
df_wettest = df_prcp_agg

for y in years_to_avg:
    df_prcp_agg_temp = df_prcp_agg.groupby(['station']).rolling(y,y, center=True,on='year').mean().reset_index()
    df_prcp_agg_temp = df_prcp_agg_temp.drop(columns="level_1")
    df_prcp_agg_temp['years_averaged'] = y
    df_wettest = pd.concat([df_wettest,df_prcp_agg_temp],axis=0).drop_duplicates()

df_agg = df_agg.merge(df_wettest, how="outer",  left_on=['station', 'year','years_averaged'], right_on=['station', 'year','years_averaged']).drop_duplicates()

# df_prcp_agg_5 = df_prcp_agg.groupby(['station']).rolling(5,5, center=True,on='year').mean().reset_index()
# df_prcp_agg_5 = df_prcp_agg_5.rename(columns={'prcp_wettest_mo' : 'prcp_wettest_mo_5yr'})
# df_prcp_agg_5 = df_prcp_agg_5.drop(columns="level_1")
# df_agg = df_agg.merge(df_prcp_agg_5, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
# df_prcp_agg_10 = df_prcp_agg.groupby(['station']).rolling(10,10, center=True,on='year').mean().reset_index()
# df_prcp_agg_10 = df_prcp_agg_10.rename(columns={'prcp_wettest_mo' : 'prcp_wettest_mo_10yr'})
# df_prcp_agg_10 = df_prcp_agg_10.drop(columns="level_1")
# df_agg = df_agg.merge(df_prcp_agg_10, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
# df_prcp_agg_15 = df_prcp_agg.groupby(['station']).rolling(15,15, center=True,on='year').mean().reset_index()
# df_prcp_agg_15 = df_prcp_agg_15.rename(columns={'prcp_wettest_mo' : 'prcp_wettest_mo_15yr'})
# df_prcp_agg_15 = df_prcp_agg_15.drop(columns="level_1")
# df_agg = df_agg.merge(df_prcp_agg_15, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
# df_prcp_agg_20 = df_prcp_agg.groupby(['station']).rolling(20,20, center=True,on='year').mean().reset_index()
# df_prcp_agg_20 = df_prcp_agg_20.rename(columns={'prcp_wettest_mo' : 'prcp_wettest_mo_20yr'})
# df_prcp_agg_20 = df_prcp_agg_20.drop(columns="level_1")
# df_agg = df_agg.merge(df_prcp_agg_20, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
# df_prcp_agg_30 = df_prcp_agg.groupby(['station']).rolling(30,30, center=True,on='year').mean().reset_index()
# df_prcp_agg_30 = df_prcp_agg_30.rename(columns={'prcp_wettest_mo' : 'prcp_wettest_mo_30yr'})
# df_prcp_agg_30 = df_prcp_agg_30.drop(columns="level_1")
# df_agg = df_agg.merge(df_prcp_agg_30, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])

df_prcp_agg = df_prcp.groupby(['station', 'year'])[['value']].sum().reset_index()
df_prcp_agg = df_prcp_agg.rename(columns={'value': 'annual_prcp(r)'})
df_prcp_agg['years_averaged'] = 1
df_prcp_agg['years_averaged'] = df_prcp_agg['years_averaged'].astype('int')
df_tot = df_prcp_agg

for y in years_to_avg:
    df_prcp_agg_temp = df_prcp_agg.groupby(['station']).rolling(y,y, center=True,on='year').mean().reset_index()
    df_prcp_agg_temp = df_prcp_agg_temp.drop(columns="level_1")
    df_prcp_agg_temp['years_averaged'] = y
    df_tot = pd.concat([df_tot,df_prcp_agg_temp],axis=0).drop_duplicates()

df_agg = df_agg.merge(df_tot, how="outer",  left_on=['station', 'year','years_averaged'], right_on=['station', 'year','years_averaged']).drop_duplicates()

# df_prcp_agg_5 = df_prcp_agg.groupby(['station']).rolling(5,5, center=True,on='year').mean().reset_index()
# df_prcp_agg_5 = df_prcp_agg_5.rename(columns={'annual_prcp(r)' : 'annual_prcp(r)_5yr'})
# df_prcp_agg_5 = df_prcp_agg_5.drop(columns="level_1")
# df_agg = df_agg.merge(df_prcp_agg_5, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
# df_prcp_agg_10 = df_prcp_agg.groupby(['station']).rolling(10,10, center=True,on='year').mean().reset_index()
# df_prcp_agg_10 = df_prcp_agg_10.rename(columns={'annual_prcp(r)' : 'annual_prcp(r)_10yr'})
# df_prcp_agg_10 = df_prcp_agg_10.drop(columns="level_1")
# df_agg = df_agg.merge(df_prcp_agg_10, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
# df_prcp_agg_15 = df_prcp_agg.groupby(['station']).rolling(15,15, center=True,on='year').mean().reset_index()
# df_prcp_agg_15 = df_prcp_agg_15.rename(columns={'annual_prcp(r)' : 'annual_prcp(r)_15yr'})
# df_prcp_agg_15 = df_prcp_agg_15.drop(columns="level_1")
# df_agg = df_agg.merge(df_prcp_agg_15, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
# df_prcp_agg_20 = df_prcp_agg.groupby(['station']).rolling(20,20, center=True,on='year').mean().reset_index()
# df_prcp_agg_20 = df_prcp_agg_20.rename(columns={'annual_prcp(r)' : 'annual_prcp(r)_20yr'})
# df_prcp_agg_20 = df_prcp_agg_20.drop(columns="level_1")
# df_agg = df_agg.merge(df_prcp_agg_20, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
# df_prcp_agg_30 = df_prcp_agg.groupby(['station']).rolling(30,30, center=True,on='year').mean().reset_index()
# df_prcp_agg_30 = df_prcp_agg_30.rename(columns={'annual_prcp(r)' : 'annual_prcp(r)_30yr'})
# df_prcp_agg_30 = df_prcp_agg_30.drop(columns="level_1")
# df_agg = df_agg.merge(df_prcp_agg_30, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])

df_prcp_summer = df_prcp[(df_prcp['year_half'] == 'summer')]

df_prcp_summer_agg = df_prcp_summer.groupby(['station', 'year'])[['value']].sum().reset_index()
df_prcp_summer_agg = df_prcp_summer_agg.rename(columns={'value': 'prcp_summer'})
df_prcp_summer_agg['years_averaged'] = 1
df_prcp_summer_agg['years_averaged'] = df_prcp_agg['years_averaged'].astype('int')
df_sum_tot = df_prcp_summer_agg

for y in years_to_avg:
    df_prcp_summer_agg_temp = df_prcp_summer_agg.groupby(['station']).rolling(y,y, center=True,on='year').mean().reset_index()
    df_prcp_summer_agg_temp = df_prcp_summer_agg_temp.drop(columns="level_1")
    df_prcp_summer_agg_temp['years_averaged'] = y
    df_sum_tot = pd.concat([df_sum_tot,df_prcp_summer_agg_temp],axis=0).drop_duplicates()

df_agg = df_agg.merge(df_sum_tot, how="outer",  left_on=['station', 'year','years_averaged'], right_on=['station', 'year','years_averaged']).drop_duplicates()

# df_prcp_agg_5 = df_prcp_summer_agg.groupby(['station']).rolling(5,5, center=True,on='year').mean().reset_index()
# df_prcp_agg_5 = df_prcp_agg_5.rename(columns={'prcp_summer' : 'prcp_summer_5yr'})
# df_prcp_agg_5 = df_prcp_agg_5.drop(columns="level_1")
# df_agg = df_agg.merge(df_prcp_agg_5, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
# df_prcp_agg_10 = df_prcp_summer_agg.groupby(['station']).rolling(10,10, center=True,on='year').mean().reset_index()
# df_prcp_agg_10 = df_prcp_agg_10.rename(columns={'prcp_summer' : 'prcp_summer_10yr'})
# df_prcp_agg_10 = df_prcp_agg_10.drop(columns="level_1")
# df_agg = df_agg.merge(df_prcp_agg_10, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
# df_prcp_agg_15 = df_prcp_summer_agg.groupby(['station']).rolling(15,15, center=True,on='year').mean().reset_index()
# df_prcp_agg_15 = df_prcp_agg_15.rename(columns={'prcp_summer' : 'prcp_summer_15yr'})
# df_prcp_agg_15 = df_prcp_agg_15.drop(columns="level_1")
# df_agg = df_agg.merge(df_prcp_agg_15, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
# df_prcp_agg_20 = df_prcp_summer_agg.groupby(['station']).rolling(20,20, center=True,on='year').mean().reset_index()
# df_prcp_agg_20 = df_prcp_agg_20.rename(columns={'prcp_summer' : 'prcp_summer_20yr'})
# df_prcp_agg_20 = df_prcp_agg_20.drop(columns="level_1")
# df_agg = df_agg.merge(df_prcp_agg_20, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
# df_prcp_agg_30 = df_prcp_summer_agg.groupby(['station']).rolling(30,30, center=True,on='year').mean().reset_index()
# df_prcp_agg_30 = df_prcp_agg_30.rename(columns={'prcp_summer' : 'prcp_summer_30yr'})
# df_prcp_agg_30 = df_prcp_agg_30.drop(columns="level_1")
# df_agg = df_agg.merge(df_prcp_agg_30, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])

df_prcp_summer_agg = df_prcp_summer.groupby(['station', 'year'])[['value']].min().reset_index()
df_prcp_summer_agg = df_prcp_summer_agg.rename(columns={'value': 'prcp_driest_mo_summer'})
df_prcp_summer_agg['years_averaged'] = 1
df_prcp_summer_agg['years_averaged'] = df_prcp_agg['years_averaged'].astype('int')
df_sum_dry = df_prcp_summer_agg

for y in years_to_avg:
    df_prcp_summer_agg_temp = df_prcp_summer_agg.groupby(['station']).rolling(y,y, center=True,on='year').mean().reset_index()
    df_prcp_summer_agg_temp = df_prcp_summer_agg_temp.drop(columns="level_1")
    df_prcp_summer_agg_temp['years_averaged'] = y
    df_sum_dry = pd.concat([df_sum_dry,df_prcp_summer_agg_temp],axis=0).drop_duplicates()

df_agg = df_agg.merge(df_sum_dry, how="outer",  left_on=['station', 'year','years_averaged'], right_on=['station', 'year','years_averaged']).drop_duplicates()

# df_prcp_agg_5 = df_prcp_summer_agg.groupby(['station']).rolling(5,5, center=True,on='year').mean().reset_index()
# df_prcp_agg_5 = df_prcp_agg_5.rename(columns={'prcp_driest_mo_summer' : 'prcp_driest_mo_summer_5yr'})
# df_prcp_agg_5 = df_prcp_agg_5.drop(columns="level_1")
# df_agg = df_agg.merge(df_prcp_agg_5, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
# df_prcp_agg_10 = df_prcp_summer_agg.groupby(['station']).rolling(10,10, center=True,on='year').mean().reset_index()
# df_prcp_agg_10 = df_prcp_agg_10.rename(columns={'prcp_driest_mo_summer' : 'prcp_driest_mo_summer_10yr'})
# df_prcp_agg_10 = df_prcp_agg_10.drop(columns="level_1")
# df_agg = df_agg.merge(df_prcp_agg_10, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
# df_prcp_agg_15 = df_prcp_summer_agg.groupby(['station']).rolling(15,15, center=True,on='year').mean().reset_index()
# df_prcp_agg_15 = df_prcp_agg_15.rename(columns={'prcp_driest_mo_summer' : 'prcp_driest_mo_summer_15yr'})
# df_prcp_agg_15 = df_prcp_agg_15.drop(columns="level_1")
# df_agg = df_agg.merge(df_prcp_agg_15, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
# df_prcp_agg_20 = df_prcp_summer_agg.groupby(['station']).rolling(20,20, center=True,on='year').mean().reset_index()
# df_prcp_agg_20 = df_prcp_agg_20.rename(columns={'prcp_driest_mo_summer' : 'prcp_driest_mo_summer_20yr'})
# df_prcp_agg_20 = df_prcp_agg_20.drop(columns="level_1")
# df_agg = df_agg.merge(df_prcp_agg_20, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
# df_prcp_agg_30 = df_prcp_summer_agg.groupby(['station']).rolling(30,30, center=True,on='year').mean().reset_index()
# df_prcp_agg_30 = df_prcp_agg_30.rename(columns={'prcp_driest_mo_summer' : 'prcp_driest_mo_summer_30yr'})
# df_prcp_agg_30 = df_prcp_agg_30.drop(columns="level_1")
# df_agg = df_agg.merge(df_prcp_agg_30, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])

df_prcp_summer_agg = df_prcp_summer.groupby(['station', 'year'])[['value']].max().reset_index()
df_prcp_summer_agg = df_prcp_summer_agg.rename(columns={'value': 'prcp_wettest_mo_summer'})
df_prcp_summer_agg['years_averaged'] = 1
df_prcp_summer_agg['years_averaged'] = df_prcp_agg['years_averaged'].astype('int')
df_sum_wet = df_prcp_summer_agg

for y in years_to_avg:
    df_prcp_summer_agg_temp = df_prcp_summer_agg.groupby(['station']).rolling(y,y, center=True,on='year').mean().reset_index()
    df_prcp_summer_agg_temp = df_prcp_summer_agg_temp.drop(columns="level_1")
    df_prcp_summer_agg_temp['years_averaged'] = y
    df_sum_wet = pd.concat([df_sum_wet,df_prcp_summer_agg_temp],axis=0).drop_duplicates()

df_agg = df_agg.merge(df_sum_wet, how="outer",  left_on=['station', 'year','years_averaged'], right_on=['station', 'year','years_averaged']).drop_duplicates()

# df_agg = df_agg.merge(df_prcp_summer_agg, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
# df_prcp_agg_5 = df_prcp_summer_agg.groupby(['station']).rolling(5,5, center=True,on='year').mean().reset_index()
# df_prcp_agg_5 = df_prcp_agg_5.rename(columns={'prcp_wettest_mo_summer' : 'prcp_wettest_mo_summer_5yr'})
# df_prcp_agg_5 = df_prcp_agg_5.drop(columns="level_1")
# df_agg = df_agg.merge(df_prcp_agg_5, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
# df_prcp_agg_10 = df_prcp_summer_agg.groupby(['station']).rolling(10,10, center=True,on='year').mean().reset_index()
# df_prcp_agg_10 = df_prcp_agg_10.rename(columns={'prcp_wettest_mo_summer' : 'prcp_wettest_mo_summer_10yr'})
# df_prcp_agg_10 = df_prcp_agg_10.drop(columns="level_1")
# df_agg = df_agg.merge(df_prcp_agg_10, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
# df_prcp_agg_15 = df_prcp_summer_agg.groupby(['station']).rolling(15,15, center=True,on='year').mean().reset_index()
# df_prcp_agg_15 = df_prcp_agg_15.rename(columns={'prcp_wettest_mo_summer' : 'prcp_wettest_mo_summer_15yr'})
# df_prcp_agg_15 = df_prcp_agg_15.drop(columns="level_1")
# df_agg = df_agg.merge(df_prcp_agg_15, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
# df_prcp_agg_20 = df_prcp_summer_agg.groupby(['station']).rolling(20,20, center=True,on='year').mean().reset_index()
# df_prcp_agg_20 = df_prcp_agg_20.rename(columns={'prcp_wettest_mo_summer' : 'prcp_wettest_mo_summer_20yr'})
# df_prcp_agg_20 = df_prcp_agg_20.drop(columns="level_1")
# df_agg = df_agg.merge(df_prcp_agg_20, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
# df_prcp_agg_30 = df_prcp_summer_agg.groupby(['station']).rolling(30,30, center=True,on='year').mean().reset_index()
# df_prcp_agg_30 = df_prcp_agg_30.rename(columns={'prcp_wettest_mo_summer' : 'prcp_wettest_mo_summer_30yr'})
# df_prcp_agg_30 = df_prcp_agg_30.drop(columns="level_1")
# df_agg = df_agg.merge(df_prcp_agg_30, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])

df_prcp_winter = df_prcp[(df_prcp['year_half'] == 'winter')]

df_prcp_winter_agg = df_prcp_winter.groupby(['station', 'year'])[['value']].sum().reset_index()
df_prcp_winter_agg = df_prcp_winter_agg.rename(columns={'value': 'prcp_winter'})
df_prcp_winter_agg['years_averaged'] = 1
df_prcp_winter_agg['years_averaged'] = df_prcp_agg['years_averaged'].astype('int')
df_win_tot = df_prcp_winter_agg

for y in years_to_avg:
    df_prcp_winter_agg_temp = df_prcp_winter_agg.groupby(['station']).rolling(y,y, center=True,on='year').mean().reset_index()
    df_prcp_winter_agg_temp = df_prcp_winter_agg_temp.drop(columns="level_1")
    df_prcp_winter_agg_temp['years_averaged'] = y
    df_win_tot = pd.concat([df_win_tot,df_prcp_winter_agg_temp],axis=0).drop_duplicates()

df_agg = df_agg.merge(df_win_tot, how="outer",  left_on=['station', 'year','years_averaged'], right_on=['station', 'year','years_averaged']).drop_duplicates()

# df_prcp_agg_5 = df_prcp_winter_agg.groupby(['station']).rolling(5,5, center=True,on='year').mean().reset_index()
# df_prcp_agg_5 = df_prcp_agg_5.rename(columns={'prcp_winter' : 'prcp_winter_5yr'})
# df_prcp_agg_5 = df_prcp_agg_5.drop(columns="level_1")
# df_agg = df_agg.merge(df_prcp_agg_5, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
# df_prcp_agg_10 = df_prcp_winter_agg.groupby(['station']).rolling(10,10, center=True,on='year').mean().reset_index()
# df_prcp_agg_10 = df_prcp_agg_10.rename(columns={'prcp_winter' : 'prcp_winter_10yr'})
# df_prcp_agg_10 = df_prcp_agg_10.drop(columns="level_1")
# df_agg = df_agg.merge(df_prcp_agg_10, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
# df_prcp_agg_15 = df_prcp_winter_agg.groupby(['station']).rolling(15,15, center=True,on='year').mean().reset_index()
# df_prcp_agg_15 = df_prcp_agg_15.rename(columns={'prcp_winter' : 'prcp_winter_15yr'})
# df_prcp_agg_15 = df_prcp_agg_15.drop(columns="level_1")
# df_agg = df_agg.merge(df_prcp_agg_15, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
# df_prcp_agg_20 = df_prcp_winter_agg.groupby(['station']).rolling(20,20, center=True,on='year').mean().reset_index()
# df_prcp_agg_20 = df_prcp_agg_20.rename(columns={'prcp_winter' : 'prcp_winter_20yr'})
# df_prcp_agg_20 = df_prcp_agg_20.drop(columns="level_1")
# df_agg = df_agg.merge(df_prcp_agg_20, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
# df_prcp_agg_30 = df_prcp_winter_agg.groupby(['station']).rolling(30,30, center=True,on='year').mean().reset_index()
# df_prcp_agg_30 = df_prcp_agg_30.rename(columns={'prcp_winter' : 'prcp_winter_30yr'})
# df_prcp_agg_30 = df_prcp_agg_30.drop(columns="level_1")
# df_agg = df_agg.merge(df_prcp_agg_30, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])

df_prcp_winter_agg = df_prcp_winter.groupby(['station', 'year'])[['value']].min().reset_index()
df_prcp_winter_agg = df_prcp_winter_agg.rename(columns={'value': 'prcp_driest_mo_winter'})
df_prcp_winter_agg['years_averaged'] = 1
df_prcp_winter_agg['years_averaged'] = df_prcp_agg['years_averaged'].astype('int')
df_win_dry = df_prcp_winter_agg

for y in years_to_avg:
    df_prcp_winter_agg_temp = df_prcp_winter_agg.groupby(['station']).rolling(y,y, center=True,on='year').mean().reset_index()
    df_prcp_winter_agg_temp = df_prcp_winter_agg_temp.drop(columns="level_1")
    df_prcp_winter_agg_temp['years_averaged'] = y
    df_win_dry = pd.concat([df_win_dry,df_prcp_winter_agg_temp],axis=0).drop_duplicates()

df_agg = df_agg.merge(df_win_dry, how="outer",  left_on=['station', 'year','years_averaged'], right_on=['station', 'year','years_averaged']).drop_duplicates()

# df_prcp_agg_5 = df_prcp_winter_agg.groupby(['station']).rolling(5, 5, center=True, on='year').mean().reset_index()
# df_prcp_agg_5 = df_prcp_agg_5.rename(columns={'prcp_driest_mo_winter': 'prcp_driest_mo_winter_5yr'})
# df_prcp_agg_5 = df_prcp_agg_5.drop(columns="level_1")
# df_agg = df_agg.merge(df_prcp_agg_5, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
# df_prcp_agg_10 = df_prcp_winter_agg.groupby(['station']).rolling(10, 10, center=True, on='year').mean().reset_index()
# df_prcp_agg_10 = df_prcp_agg_10.rename(columns={'prcp_driest_mo_winter': 'prcp_driest_mo_winter_10yr'})
# df_prcp_agg_10 = df_prcp_agg_10.drop(columns="level_1")
# df_agg = df_agg.merge(df_prcp_agg_10, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
# df_prcp_agg_15 = df_prcp_winter_agg.groupby(['station']).rolling(15, 15, center=True, on='year').mean().reset_index()
# df_prcp_agg_15 = df_prcp_agg_15.rename(columns={'prcp_driest_mo_winter': 'prcp_driest_mo_winter_15yr'})
# df_prcp_agg_15 = df_prcp_agg_15.drop(columns="level_1")
# df_agg = df_agg.merge(df_prcp_agg_15, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
# df_prcp_agg_20 = df_prcp_winter_agg.groupby(['station']).rolling(20, 20, center=True, on='year').mean().reset_index()
# df_prcp_agg_20 = df_prcp_agg_20.rename(columns={'prcp_driest_mo_winter': 'prcp_driest_mo_winter_20yr'})
# df_prcp_agg_20 = df_prcp_agg_20.drop(columns="level_1")
# df_agg = df_agg.merge(df_prcp_agg_20, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
# df_prcp_agg_30 = df_prcp_winter_agg.groupby(['station']).rolling(30, 30, center=True, on='year').mean().reset_index()
# df_prcp_agg_30 = df_prcp_agg_30.rename(columns={'prcp_driest_mo_winter': 'prcp_driest_mo_winter_30yr'})
# df_prcp_agg_30 = df_prcp_agg_30.drop(columns="level_1")
# df_agg = df_agg.merge(df_prcp_agg_30, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])

df_prcp_winter_agg = df_prcp_winter.groupby(['station', 'year'])[['value']].max().reset_index()
df_prcp_winter_agg = df_prcp_winter_agg.rename(columns={'value': 'prcp_wettest_mo_winter'})
df_prcp_winter_agg['years_averaged'] = 1
df_prcp_winter_agg['years_averaged'] = df_prcp_agg['years_averaged'].astype('int')
df_win_wet = df_prcp_winter_agg

for y in years_to_avg:
    df_prcp_winter_agg_temp = df_prcp_winter_agg.groupby(['station']).rolling(y,y, center=True,on='year').mean().reset_index()
    df_prcp_winter_agg_temp = df_prcp_winter_agg_temp.drop(columns="level_1")
    df_prcp_winter_agg_temp['years_averaged'] = y
    df_win_wet = pd.concat([df_win_wet,df_prcp_winter_agg_temp],axis=0).drop_duplicates()

df_agg = df_agg.merge(df_win_wet, how="outer",  left_on=['station', 'year','years_averaged'], right_on=['station', 'year','years_averaged']).drop_duplicates()

# df_prcp_agg_5 = df_prcp_winter_agg.groupby(['station']).rolling(5, 5, center=True, on='year').mean().reset_index()
# df_prcp_agg_5 = df_prcp_agg_5.rename(columns={'prcp_wettest_mo_winter': 'prcp_wettest_mo_winter_5yr'})
# df_prcp_agg_5 = df_prcp_agg_5.drop(columns="level_1")
# df_agg = df_agg.merge(df_prcp_agg_5, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
# df_prcp_agg_10 = df_prcp_winter_agg.groupby(['station']).rolling(10, 10, center=True, on='year').mean().reset_index()
# df_prcp_agg_10 = df_prcp_agg_10.rename(columns={'prcp_wettest_mo_winter': 'prcp_wettest_mo_winter_10yr'})
# df_prcp_agg_10 = df_prcp_agg_10.drop(columns="level_1")
# df_agg = df_agg.merge(df_prcp_agg_10, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
# df_prcp_agg_15 = df_prcp_winter_agg.groupby(['station']).rolling(15, 15, center=True, on='year').mean().reset_index()
# df_prcp_agg_15 = df_prcp_agg_15.rename(columns={'prcp_wettest_mo_winter': 'prcp_wettest_mo_winter_15yr'})
# df_prcp_agg_15 = df_prcp_agg_15.drop(columns="level_1")
# df_agg = df_agg.merge(df_prcp_agg_15, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
# df_prcp_agg_20 = df_prcp_winter_agg.groupby(['station']).rolling(20, 20, center=True, on='year').mean().reset_index()
# df_prcp_agg_20 = df_prcp_agg_20.rename(columns={'prcp_wettest_mo_winter': 'prcp_wettest_mo_winter_20yr'})
# df_prcp_agg_20 = df_prcp_agg_20.drop(columns="level_1")
# df_agg = df_agg.merge(df_prcp_agg_20, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
# df_prcp_agg_30 = df_prcp_winter_agg.groupby(['station']).rolling(30, 30, center=True, on='year').mean().reset_index()
# df_prcp_agg_30 = df_prcp_agg_30.rename(columns={'prcp_wettest_mo_winter': 'prcp_wettest_mo_winter_30yr'})
# df_prcp_agg_30 = df_prcp_agg_30.drop(columns="level_1")
# df_agg = df_agg.merge(df_prcp_agg_30, how="outer", left_on=['station', 'year'], right_on=['station', 'year'])
del_list = [df, df_prcp_winter_agg, df_prcp_winter, df_prcp_summer, df_prcp_summer_agg, df_prcp_agg,df_prcp_agg_temp, df_prcp, df_driest, df_wettest, df_win_dry,df_win_tot,df_win_wet,df_sum_dry,df_sum_tot,df_sum_wet,df_prcp_summer_agg_temp, df_prcp_winter_agg_temp, df_tot]
del del_list
del df
del df_prcp_winter_agg
del df_prcp_winter
del df_prcp_summer
del df_prcp_summer_agg
del df_prcp_agg
del df_prcp_agg_temp
del df_prcp
del df_sum_wet
del df_sum_tot
del df_sum_dry
del df_win_wet
del df_win_tot
del df_win_dry
del df_wettest
del df_driest
del df_prcp_summer_agg_temp
del df_prcp_winter_agg_temp
del df_tot

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
#
# def koppen_5yr(s):
#     # run class B first because Arid climates can intersect with the other types, but we want B to take priority over the temp rules
#     if (((s['annual_prcp(r)_5yr'] < (10 * (2 * s['avg_annual_temp(t)_5yr']))) & (
#             s['prcp_winter_5yr'] >= (.7 * s['annual_prcp(r)_5yr']))) | (
#             (s['annual_prcp(r)_5yr'] < (10 * ((2 * s['avg_annual_temp(t)_5yr']) + 28))) & (
#             s['prcp_summer_5yr'] >= (.7 * s['annual_prcp(r)_5yr']))) | (
#     (s['annual_prcp(r)_5yr'] < (10 * ((2 * s['avg_annual_temp(t)_5yr']) + 14))))):
#         if (((s['annual_prcp(r)_5yr'] < (5 * (2 * s['avg_annual_temp(t)_5yr']))) & (
#                 s['prcp_winter_5yr'] >= (.7 * s['annual_prcp(r)_5yr']))) | (
#                 (s['annual_prcp(r)_5yr'] < (5 * ((2 * s['avg_annual_temp(t)_5yr']) + 28))) & (
#                 s['prcp_summer_5yr'] >= (.7 * s['annual_prcp(r)_5yr']))) | (
#         (s['annual_prcp(r)_5yr'] < (5 * ((2 * s['avg_annual_temp(t)_5yr']) + 14))))):
#             if (s['avg_annual_temp(t)_5yr'] >= 18):
#                 return 'BWh'
#             if (s['avg_annual_temp(t)_5yr'] < 18):
#                 return 'BWk'
#         if (((s['annual_prcp(r)_5yr'] >= (5 * (2 * s['avg_annual_temp(t)_5yr']))) & (
#                 s['prcp_winter_5yr'] >= (.7 * s['annual_prcp(r)_5yr']))) | (
#                 (s['annual_prcp(r)_5yr'] >= (5 * ((2 * s['avg_annual_temp(t)_5yr']) + 28))) & (
#                 s['prcp_summer_5yr'] >= (.7 * s['annual_prcp(r)_5yr']))) | (
#         (s['annual_prcp(r)_5yr'] >= (5 * ((2 * s['avg_annual_temp(t)_5yr']) + 14))))):
#             if (s['avg_annual_temp(t)_5yr'] >= 18):
#                 return 'BSh'
#             if (s['avg_annual_temp(t)_5yr'] < 18):
#                 return 'BSk'
#     elif (s['temp_coldest_mo_5yr'] >= 18):
#         if (s['prcp_driest_mo_5yr'] >= 60):
#             return 'Af'
#         if ((s['prcp_driest_mo_5yr'] < 60) & (s['prcp_driest_mo_5yr'] >= (100 - (s['annual_prcp(r)_5yr'] / 25)))):
#             return 'Am'
#         if ((s['prcp_driest_mo_5yr'] < 60) & (s['prcp_driest_mo_5yr'] < (100 - (s['annual_prcp(r)_5yr'] / 25)))):
#             return 'Aw'
#     elif ((s['temp_coldest_mo_5yr'] > 0) & (s['temp_coldest_mo_5yr'] < 18)):
#         if ((s['prcp_driest_mo_summer_5yr'] < 40) & (s['prcp_driest_mo_summer_5yr'] < (s['prcp_wettest_mo_winter_5yr'] / 3))):
#             if (s['temp_hottest_mo_5yr'] >= 22):
#                 return 'Csa'
#             elif (s['num_mo_btw_10-22C_5yr'] >= 4):
#                 return 'Csb'
#             elif (s['num_mo_btw_10-22C_5yr'] < 4):
#                 return 'Csc'
#             else:
#                 return 'Cs'
#         elif (s['prcp_driest_mo_winter_5yr'] < (s['prcp_wettest_mo_summer_5yr'] / 10)):
#             if (s['temp_hottest_mo_5yr'] >= 22):
#                 return 'Cwa'
#             elif (s['num_mo_btw_10-22C_5yr'] >= 4):
#                 return 'Cwb'
#             elif (s['num_mo_btw_10-22C_5yr'] < 4):
#                 return 'Cwc'
#             else:
#                 return 'Cw'
#         else:
#             if (s['temp_hottest_mo_5yr'] >= 22):
#                 return 'Cfa'
#             elif (s['num_mo_btw_10-22C_5yr'] >= 4):
#                 return 'Cfb'
#             elif (s['num_mo_btw_10-22C_5yr'] < 4):
#                 return 'Cfc'
#             else:
#                 return 'Cf'
#     elif ((s['temp_coldest_mo_5yr'] <= 0) & (s['temp_hottest_mo_5yr'] > 10)):
#         if ((s['prcp_driest_mo_summer_5yr'] < 40) & (s['prcp_driest_mo_summer_5yr'] < (s['prcp_wettest_mo_winter_5yr'] / 3))):
#             if (s['temp_hottest_mo_5yr'] >= 22):
#                 return 'Dsa'
#             elif (s['num_mo_btw_10-22C_5yr'] >= 4):
#                 return 'Dsb'
#             elif (s['temp_coldest_mo_5yr'] < -38):
#                 return 'Dsd'
#             else:
#                 return 'Dsc'
#         elif (s['prcp_driest_mo_winter_5yr'] < (s['prcp_wettest_mo_summer_5yr'] / 10)):
#             if (s['temp_hottest_mo_5yr'] >= 22):
#                 return 'Dwa'
#             elif (s['num_mo_btw_10-22C_5yr'] >= 4):
#                 return 'Dwb'
#             elif (s['temp_coldest_mo_5yr'] < -38):
#                 return 'Dwd'
#             else:
#                 return 'Dwc'
#         else:
#             if (s['temp_hottest_mo_5yr'] >= 22):
#                 return 'Dfa'
#             elif (s['num_mo_btw_10-22C_5yr'] >= 4):
#                 return 'Dfb'
#             elif (s['temp_coldest_mo_5yr'] < -38):
#                 return 'Dfd'
#             else:
#                 return 'Dfc'
#     elif (s['temp_hottest_mo_5yr'] <= 10):
#         if (s['temp_hottest_mo_5yr'] > 0):
#             return 'ET'
#         if (s['temp_hottest_mo_5yr'] <= 0):
#             return 'EF'
#     elif (s['elevation'] > 1500):
#         return 'H'
#
# def koppen_10yr(s):
#     # run class B first because Arid climates can intersect with the other types, but we want B to take priority over the temp rules
#     if (((s['annual_prcp(r)_10yr'] < (10 * (2 * s['avg_annual_temp(t)_10yr']))) & (
#             s['prcp_winter_10yr'] >= (.7 * s['annual_prcp(r)_10yr']))) | (
#             (s['annual_prcp(r)_10yr'] < (10 * ((2 * s['avg_annual_temp(t)_10yr']) + 28))) & (
#             s['prcp_summer_10yr'] >= (.7 * s['annual_prcp(r)_10yr']))) | (
#     (s['annual_prcp(r)_10yr'] < (10 * ((2 * s['avg_annual_temp(t)_10yr']) + 14))))):
#         if (((s['annual_prcp(r)_10yr'] < (5 * (2 * s['avg_annual_temp(t)_10yr']))) & (
#                 s['prcp_winter_10yr'] >= (.7 * s['annual_prcp(r)_10yr']))) | (
#                 (s['annual_prcp(r)_10yr'] < (5 * ((2 * s['avg_annual_temp(t)_10yr']) + 28))) & (
#                 s['prcp_summer_10yr'] >= (.7 * s['annual_prcp(r)_10yr']))) | (
#         (s['annual_prcp(r)_10yr'] < (5 * ((2 * s['avg_annual_temp(t)_10yr']) + 14))))):
#             if (s['avg_annual_temp(t)_10yr'] >= 18):
#                 return 'BWh'
#             if (s['avg_annual_temp(t)_10yr'] < 18):
#                 return 'BWk'
#         if (((s['annual_prcp(r)_10yr'] >= (5 * (2 * s['avg_annual_temp(t)_10yr']))) & (
#                 s['prcp_winter_10yr'] >= (.7 * s['annual_prcp(r)_10yr']))) | (
#                 (s['annual_prcp(r)_10yr'] >= (5 * ((2 * s['avg_annual_temp(t)_10yr']) + 28))) & (
#                 s['prcp_summer_10yr'] >= (.7 * s['annual_prcp(r)_10yr']))) | (
#         (s['annual_prcp(r)_10yr'] >= (5 * ((2 * s['avg_annual_temp(t)_10yr']) + 14))))):
#             if (s['avg_annual_temp(t)_10yr'] >= 18):
#                 return 'BSh'
#             if (s['avg_annual_temp(t)_10yr'] < 18):
#                 return 'BSk'
#     elif (s['temp_coldest_mo_10yr'] >= 18):
#         if (s['prcp_driest_mo_10yr'] >= 60):
#             return 'Af'
#         if ((s['prcp_driest_mo_10yr'] < 60) & (s['prcp_driest_mo_10yr'] >= (100 - (s['annual_prcp(r)_10yr'] / 25)))):
#             return 'Am'
#         if ((s['prcp_driest_mo_10yr'] < 60) & (s['prcp_driest_mo_10yr'] < (100 - (s['annual_prcp(r)_10yr'] / 25)))):
#             return 'Aw'
#     elif ((s['temp_coldest_mo_10yr'] > 0) & (s['temp_coldest_mo_10yr'] < 18)):
#         if ((s['prcp_driest_mo_summer_10yr'] < 40) & (s['prcp_driest_mo_summer_10yr'] < (s['prcp_wettest_mo_winter_10yr'] / 3))):
#             if (s['temp_hottest_mo_10yr'] >= 22):
#                 return 'Csa'
#             elif (s['num_mo_btw_10-22C_10yr'] >= 4):
#                 return 'Csb'
#             elif (s['num_mo_btw_10-22C_10yr'] < 4):
#                 return 'Csc'
#             else:
#                 return 'Cs'
#         elif (s['prcp_driest_mo_winter_10yr'] < (s['prcp_wettest_mo_summer_10yr'] / 10)):
#             if (s['temp_hottest_mo_10yr'] >= 22):
#                 return 'Cwa'
#             elif (s['num_mo_btw_10-22C_10yr'] >= 4):
#                 return 'Cwb'
#             elif (s['num_mo_btw_10-22C_10yr'] < 4):
#                 return 'Cwc'
#             else:
#                 return 'Cw'
#         else:
#             if (s['temp_hottest_mo_10yr'] >= 22):
#                 return 'Cfa'
#             elif (s['num_mo_btw_10-22C_10yr'] >= 4):
#                 return 'Cfb'
#             elif (s['num_mo_btw_10-22C_10yr'] < 4):
#                 return 'Cfc'
#             else:
#                 return 'Cf'
#     elif ((s['temp_coldest_mo_10yr'] <= 0) & (s['temp_hottest_mo_10yr'] > 10)):
#         if ((s['prcp_driest_mo_summer_10yr'] < 40) & (s['prcp_driest_mo_summer_10yr'] < (s['prcp_wettest_mo_winter_10yr'] / 3))):
#             if (s['temp_hottest_mo_10yr'] >= 22):
#                 return 'Dsa'
#             elif (s['num_mo_btw_10-22C_10yr'] >= 4):
#                 return 'Dsb'
#             elif (s['temp_coldest_mo_10yr'] < -38):
#                 return 'Dsd'
#             else:
#                 return 'Dsc'
#         elif (s['prcp_driest_mo_winter_10yr'] < (s['prcp_wettest_mo_summer_10yr'] / 10)):
#             if (s['temp_hottest_mo_10yr'] >= 22):
#                 return 'Dwa'
#             elif (s['num_mo_btw_10-22C_10yr'] >= 4):
#                 return 'Dwb'
#             elif (s['temp_coldest_mo_10yr'] < -38):
#                 return 'Dwd'
#             else:
#                 return 'Dwc'
#         else:
#             if (s['temp_hottest_mo_10yr'] >= 22):
#                 return 'Dfa'
#             elif (s['num_mo_btw_10-22C_10yr'] >= 4):
#                 return 'Dfb'
#             elif (s['temp_coldest_mo_10yr'] < -38):
#                 return 'Dfd'
#             else:
#                 return 'Dfc'
#     elif (s['temp_hottest_mo_10yr'] <= 10):
#         if (s['temp_hottest_mo_10yr'] > 0):
#             return 'ET'
#         if (s['temp_hottest_mo_10yr'] <= 0):
#             return 'EF'
#     elif (s['elevation'] > 1500):
#         return 'H'
#
# def koppen_15yr(s):
#     # run class B first because Arid climates can intersect with the other types, but we want B to take priority over the temp rules
#     if (((s['annual_prcp(r)_15yr'] < (10 * (2 * s['avg_annual_temp(t)_15yr']))) & (
#             s['prcp_winter_15yr'] >= (.7 * s['annual_prcp(r)_15yr']))) | (
#             (s['annual_prcp(r)_15yr'] < (10 * ((2 * s['avg_annual_temp(t)_15yr']) + 28))) & (
#             s['prcp_summer_15yr'] >= (.7 * s['annual_prcp(r)_15yr']))) | (
#     (s['annual_prcp(r)_15yr'] < (10 * ((2 * s['avg_annual_temp(t)_15yr']) + 14))))):
#         if (((s['annual_prcp(r)_15yr'] < (5 * (2 * s['avg_annual_temp(t)_15yr']))) & (
#                 s['prcp_winter_15yr'] >= (.7 * s['annual_prcp(r)_15yr']))) | (
#                 (s['annual_prcp(r)_15yr'] < (5 * ((2 * s['avg_annual_temp(t)_15yr']) + 28))) & (
#                 s['prcp_summer_15yr'] >= (.7 * s['annual_prcp(r)_15yr']))) | (
#         (s['annual_prcp(r)_15yr'] < (5 * ((2 * s['avg_annual_temp(t)_15yr']) + 14))))):
#             if (s['avg_annual_temp(t)_15yr'] >= 18):
#                 return 'BWh'
#             if (s['avg_annual_temp(t)_15yr'] < 18):
#                 return 'BWk'
#         if (((s['annual_prcp(r)_15yr'] >= (5 * (2 * s['avg_annual_temp(t)_15yr']))) & (
#                 s['prcp_winter_15yr'] >= (.7 * s['annual_prcp(r)_15yr']))) | (
#                 (s['annual_prcp(r)_15yr'] >= (5 * ((2 * s['avg_annual_temp(t)_15yr']) + 28))) & (
#                 s['prcp_summer_15yr'] >= (.7 * s['annual_prcp(r)_15yr']))) | (
#         (s['annual_prcp(r)_15yr'] >= (5 * ((2 * s['avg_annual_temp(t)_15yr']) + 14))))):
#             if (s['avg_annual_temp(t)_15yr'] >= 18):
#                 return 'BSh'
#             if (s['avg_annual_temp(t)_15yr'] < 18):
#                 return 'BSk'
#     elif (s['temp_coldest_mo_15yr'] >= 18):
#         if (s['prcp_driest_mo_15yr'] >= 60):
#             return 'Af'
#         if ((s['prcp_driest_mo_15yr'] < 60) & (s['prcp_driest_mo_15yr'] >= (100 - (s['annual_prcp(r)_15yr'] / 25)))):
#             return 'Am'
#         if ((s['prcp_driest_mo_15yr'] < 60) & (s['prcp_driest_mo_15yr'] < (100 - (s['annual_prcp(r)_15yr'] / 25)))):
#             return 'Aw'
#     elif ((s['temp_coldest_mo_15yr'] > 0) & (s['temp_coldest_mo_15yr'] < 18)):
#         if ((s['prcp_driest_mo_summer_15yr'] < 40) & (s['prcp_driest_mo_summer_15yr'] < (s['prcp_wettest_mo_winter_15yr'] / 3))):
#             if (s['temp_hottest_mo_15yr'] >= 22):
#                 return 'Csa'
#             elif (s['num_mo_btw_10-22C_15yr'] >= 4):
#                 return 'Csb'
#             elif (s['num_mo_btw_10-22C_15yr'] < 4):
#                 return 'Csc'
#             else:
#                 return 'Cs'
#         elif (s['prcp_driest_mo_winter_15yr'] < (s['prcp_wettest_mo_summer_15yr'] / 10)):
#             if (s['temp_hottest_mo_15yr'] >= 22):
#                 return 'Cwa'
#             elif (s['num_mo_btw_10-22C_15yr'] >= 4):
#                 return 'Cwb'
#             elif (s['num_mo_btw_10-22C_15yr'] < 4):
#                 return 'Cwc'
#             else:
#                 return 'Cw'
#         else:
#             if (s['temp_hottest_mo_15yr'] >= 22):
#                 return 'Cfa'
#             elif (s['num_mo_btw_10-22C_15yr'] >= 4):
#                 return 'Cfb'
#             elif (s['num_mo_btw_10-22C_15yr'] < 4):
#                 return 'Cfc'
#             else:
#                 return 'Cf'
#     elif ((s['temp_coldest_mo_15yr'] <= 0) & (s['temp_hottest_mo_15yr'] > 10)):
#         if ((s['prcp_driest_mo_summer_15yr'] < 40) & (s['prcp_driest_mo_summer_15yr'] < (s['prcp_wettest_mo_winter_15yr'] / 3))):
#             if (s['temp_hottest_mo_15yr'] >= 22):
#                 return 'Dsa'
#             elif (s['num_mo_btw_10-22C_15yr'] >= 4):
#                 return 'Dsb'
#             elif (s['temp_coldest_mo_15yr'] < -38):
#                 return 'Dsd'
#             else:
#                 return 'Dsc'
#         elif (s['prcp_driest_mo_winter_15yr'] < (s['prcp_wettest_mo_summer_15yr'] / 10)):
#             if (s['temp_hottest_mo_15yr'] >= 22):
#                 return 'Dwa'
#             elif (s['num_mo_btw_10-22C_15yr'] >= 4):
#                 return 'Dwb'
#             elif (s['temp_coldest_mo_15yr'] < -38):
#                 return 'Dwd'
#             else:
#                 return 'Dwc'
#         else:
#             if (s['temp_hottest_mo_15yr'] >= 22):
#                 return 'Dfa'
#             elif (s['num_mo_btw_10-22C_15yr'] >= 4):
#                 return 'Dfb'
#             elif (s['temp_coldest_mo_15yr'] < -38):
#                 return 'Dfd'
#             else:
#                 return 'Dfc'
#     elif (s['temp_hottest_mo_15yr'] <= 10):
#         if (s['temp_hottest_mo_15yr'] > 0):
#             return 'ET'
#         if (s['temp_hottest_mo_15yr'] <= 0):
#             return 'EF'
#     elif (s['elevation'] > 1500):
#         return 'H'
#
# def koppen_20yr(s):
#     # run class B first because Arid climates can intersect with the other types, but we want B to take priority over the temp rules
#     if (((s['annual_prcp(r)_20yr'] < (10 * (2 * s['avg_annual_temp(t)_20yr']))) & (
#             s['prcp_winter_20yr'] >= (.7 * s['annual_prcp(r)_20yr']))) | (
#             (s['annual_prcp(r)_20yr'] < (10 * ((2 * s['avg_annual_temp(t)_20yr']) + 28))) & (
#             s['prcp_summer_20yr'] >= (.7 * s['annual_prcp(r)_20yr']))) | (
#     (s['annual_prcp(r)_20yr'] < (10 * ((2 * s['avg_annual_temp(t)_20yr']) + 14))))):
#         if (((s['annual_prcp(r)_20yr'] < (5 * (2 * s['avg_annual_temp(t)_20yr']))) & (
#                 s['prcp_winter_20yr'] >= (.7 * s['annual_prcp(r)_20yr']))) | (
#                 (s['annual_prcp(r)_20yr'] < (5 * ((2 * s['avg_annual_temp(t)_20yr']) + 28))) & (
#                 s['prcp_summer_20yr'] >= (.7 * s['annual_prcp(r)_20yr']))) | (
#         (s['annual_prcp(r)_20yr'] < (5 * ((2 * s['avg_annual_temp(t)_20yr']) + 14))))):
#             if (s['avg_annual_temp(t)_20yr'] >= 18):
#                 return 'BWh'
#             if (s['avg_annual_temp(t)_20yr'] < 18):
#                 return 'BWk'
#         if (((s['annual_prcp(r)_20yr'] >= (5 * (2 * s['avg_annual_temp(t)_20yr']))) & (
#                 s['prcp_winter_20yr'] >= (.7 * s['annual_prcp(r)_20yr']))) | (
#                 (s['annual_prcp(r)_20yr'] >= (5 * ((2 * s['avg_annual_temp(t)_20yr']) + 28))) & (
#                 s['prcp_summer_20yr'] >= (.7 * s['annual_prcp(r)_20yr']))) | (
#         (s['annual_prcp(r)_20yr'] >= (5 * ((2 * s['avg_annual_temp(t)_20yr']) + 14))))):
#             if (s['avg_annual_temp(t)_20yr'] >= 18):
#                 return 'BSh'
#             if (s['avg_annual_temp(t)_20yr'] < 18):
#                 return 'BSk'
#     elif (s['temp_coldest_mo_20yr'] >= 18):
#         if (s['prcp_driest_mo_20yr'] >= 60):
#             return 'Af'
#         if ((s['prcp_driest_mo_20yr'] < 60) & (s['prcp_driest_mo_20yr'] >= (100 - (s['annual_prcp(r)_20yr'] / 25)))):
#             return 'Am'
#         if ((s['prcp_driest_mo_20yr'] < 60) & (s['prcp_driest_mo_20yr'] < (100 - (s['annual_prcp(r)_20yr'] / 25)))):
#             return 'Aw'
#     elif ((s['temp_coldest_mo_20yr'] > 0) & (s['temp_coldest_mo_20yr'] < 18)):
#         if ((s['prcp_driest_mo_summer_20yr'] < 40) & (s['prcp_driest_mo_summer_20yr'] < (s['prcp_wettest_mo_winter_20yr'] / 3))):
#             if (s['temp_hottest_mo_20yr'] >= 22):
#                 return 'Csa'
#             elif (s['num_mo_btw_10-22C_20yr'] >= 4):
#                 return 'Csb'
#             elif (s['num_mo_btw_10-22C_20yr'] < 4):
#                 return 'Csc'
#             else:
#                 return 'Cs'
#         elif (s['prcp_driest_mo_winter_20yr'] < (s['prcp_wettest_mo_summer_20yr'] / 10)):
#             if (s['temp_hottest_mo_20yr'] >= 22):
#                 return 'Cwa'
#             elif (s['num_mo_btw_10-22C_20yr'] >= 4):
#                 return 'Cwb'
#             elif (s['num_mo_btw_10-22C_20yr'] < 4):
#                 return 'Cwc'
#             else:
#                 return 'Cw'
#         else:
#             if (s['temp_hottest_mo_20yr'] >= 22):
#                 return 'Cfa'
#             elif (s['num_mo_btw_10-22C_20yr'] >= 4):
#                 return 'Cfb'
#             elif (s['num_mo_btw_10-22C_20yr'] < 4):
#                 return 'Cfc'
#             else:
#                 return 'Cf'
#     elif ((s['temp_coldest_mo_20yr'] <= 0) & (s['temp_hottest_mo_20yr'] > 10)):
#         if ((s['prcp_driest_mo_summer_20yr'] < 40) & (s['prcp_driest_mo_summer_20yr'] < (s['prcp_wettest_mo_winter_20yr'] / 3))):
#             if (s['temp_hottest_mo_20yr'] >= 22):
#                 return 'Dsa'
#             elif (s['num_mo_btw_10-22C_20yr'] >= 4):
#                 return 'Dsb'
#             elif (s['temp_coldest_mo_20yr'] < -38):
#                 return 'Dsd'
#             else:
#                 return 'Dsc'
#         elif (s['prcp_driest_mo_winter_20yr'] < (s['prcp_wettest_mo_summer_20yr'] / 10)):
#             if (s['temp_hottest_mo_20yr'] >= 22):
#                 return 'Dwa'
#             elif (s['num_mo_btw_10-22C_20yr'] >= 4):
#                 return 'Dwb'
#             elif (s['temp_coldest_mo_20yr'] < -38):
#                 return 'Dwd'
#             else:
#                 return 'Dwc'
#         else:
#             if (s['temp_hottest_mo_20yr'] >= 22):
#                 return 'Dfa'
#             elif (s['num_mo_btw_10-22C_20yr'] >= 4):
#                 return 'Dfb'
#             elif (s['temp_coldest_mo_20yr'] < -38):
#                 return 'Dfd'
#             else:
#                 return 'Dfc'
#     elif (s['temp_hottest_mo_20yr'] <= 10):
#         if (s['temp_hottest_mo_20yr'] > 0):
#             return 'ET'
#         if (s['temp_hottest_mo_20yr'] <= 0):
#             return 'EF'
#     elif (s['elevation'] > 1500):
#         return 'H'
#
# def koppen_30yr(s):
#     # run class B first because Arid climates can intersect with the other types, but we want B to take priority over the temp rules
#     if (((s['annual_prcp(r)_30yr'] < (10 * (2 * s['avg_annual_temp(t)_30yr']))) & (
#             s['prcp_winter_30yr'] >= (.7 * s['annual_prcp(r)_30yr']))) | (
#             (s['annual_prcp(r)_30yr'] < (10 * ((2 * s['avg_annual_temp(t)_30yr']) + 28))) & (
#             s['prcp_summer_30yr'] >= (.7 * s['annual_prcp(r)_30yr']))) | (
#     (s['annual_prcp(r)_30yr'] < (10 * ((2 * s['avg_annual_temp(t)_30yr']) + 14))))):
#         if (((s['annual_prcp(r)_30yr'] < (5 * (2 * s['avg_annual_temp(t)_30yr']))) & (
#                 s['prcp_winter_30yr'] >= (.7 * s['annual_prcp(r)_30yr']))) | (
#                 (s['annual_prcp(r)_30yr'] < (5 * ((2 * s['avg_annual_temp(t)_30yr']) + 28))) & (
#                 s['prcp_summer_30yr'] >= (.7 * s['annual_prcp(r)_30yr']))) | (
#         (s['annual_prcp(r)_30yr'] < (5 * ((2 * s['avg_annual_temp(t)_30yr']) + 14))))):
#             if (s['avg_annual_temp(t)_30yr'] >= 18):
#                 return 'BWh'
#             if (s['avg_annual_temp(t)_30yr'] < 18):
#                 return 'BWk'
#         if (((s['annual_prcp(r)_30yr'] >= (5 * (2 * s['avg_annual_temp(t)_30yr']))) & (
#                 s['prcp_winter_30yr'] >= (.7 * s['annual_prcp(r)_30yr']))) | (
#                 (s['annual_prcp(r)_30yr'] >= (5 * ((2 * s['avg_annual_temp(t)_30yr']) + 28))) & (
#                 s['prcp_summer_30yr'] >= (.7 * s['annual_prcp(r)_30yr']))) | (
#         (s['annual_prcp(r)_30yr'] >= (5 * ((2 * s['avg_annual_temp(t)_30yr']) + 14))))):
#             if (s['avg_annual_temp(t)_30yr'] >= 18):
#                 return 'BSh'
#             if (s['avg_annual_temp(t)_30yr'] < 18):
#                 return 'BSk'
#     elif (s['temp_coldest_mo_30yr'] >= 18):
#         if (s['prcp_driest_mo_30yr'] >= 60):
#             return 'Af'
#         if ((s['prcp_driest_mo_30yr'] < 60) & (s['prcp_driest_mo_30yr'] >= (100 - (s['annual_prcp(r)_30yr'] / 25)))):
#             return 'Am'
#         if ((s['prcp_driest_mo_30yr'] < 60) & (s['prcp_driest_mo_30yr'] < (100 - (s['annual_prcp(r)_30yr'] / 25)))):
#             return 'Aw'
#     elif ((s['temp_coldest_mo_30yr'] > 0) & (s['temp_coldest_mo_30yr'] < 18)):
#         if ((s['prcp_driest_mo_summer_30yr'] < 40) & (s['prcp_driest_mo_summer_30yr'] < (s['prcp_wettest_mo_winter_30yr'] / 3))):
#             if (s['temp_hottest_mo_30yr'] >= 22):
#                 return 'Csa'
#             elif (s['num_mo_btw_10-22C_30yr'] >= 4):
#                 return 'Csb'
#             elif (s['num_mo_btw_10-22C_30yr'] < 4):
#                 return 'Csc'
#             else:
#                 return 'Cs'
#         elif (s['prcp_driest_mo_winter_30yr'] < (s['prcp_wettest_mo_summer_30yr'] / 10)):
#             if (s['temp_hottest_mo_30yr'] >= 22):
#                 return 'Cwa'
#             elif (s['num_mo_btw_10-22C_30yr'] >= 4):
#                 return 'Cwb'
#             elif (s['num_mo_btw_10-22C_30yr'] < 4):
#                 return 'Cwc'
#             else:
#                 return 'Cw'
#         else:
#             if (s['temp_hottest_mo_30yr'] >= 22):
#                 return 'Cfa'
#             elif (s['num_mo_btw_10-22C_30yr'] >= 4):
#                 return 'Cfb'
#             elif (s['num_mo_btw_10-22C_30yr'] < 4):
#                 return 'Cfc'
#             else:
#                 return 'Cf'
#     elif ((s['temp_coldest_mo_30yr'] <= 0) & (s['temp_hottest_mo_30yr'] > 10)):
#         if ((s['prcp_driest_mo_summer_30yr'] < 40) & (s['prcp_driest_mo_summer_30yr'] < (s['prcp_wettest_mo_winter_30yr'] / 3))):
#             if (s['temp_hottest_mo_30yr'] >= 22):
#                 return 'Dsa'
#             elif (s['num_mo_btw_10-22C_30yr'] >= 4):
#                 return 'Dsb'
#             elif (s['temp_coldest_mo_30yr'] < -38):
#                 return 'Dsd'
#             else:
#                 return 'Dsc'
#         elif (s['prcp_driest_mo_winter_30yr'] < (s['prcp_wettest_mo_summer_30yr'] / 10)):
#             if (s['temp_hottest_mo_30yr'] >= 22):
#                 return 'Dwa'
#             elif (s['num_mo_btw_10-22C_30yr'] >= 4):
#                 return 'Dwb'
#             elif (s['temp_coldest_mo_30yr'] < -38):
#                 return 'Dwd'
#             else:
#                 return 'Dwc'
#         else:
#             if (s['temp_hottest_mo_30yr'] >= 22):
#                 return 'Dfa'
#             elif (s['num_mo_btw_10-22C_30yr'] >= 4):
#                 return 'Dfb'
#             elif (s['temp_coldest_mo_30yr'] < -38):
#                 return 'Dfd'
#             else:
#                 return 'Dfc'
#     elif (s['temp_hottest_mo_30yr'] <= 10):
#         if (s['temp_hottest_mo_30yr'] > 0):
#             return 'ET'
#         if (s['temp_hottest_mo_30yr'] <= 0):
#             return 'EF'
#     elif (s['elevation'] > 1500):
#         return 'H'

df_agg['koppen'] = df_agg.apply(koppen, axis=1)
# df_agg['koppen_5yr'] = df_agg.apply(koppen_5yr, axis=1)
# df_agg['koppen_10yr'] = df_agg.apply(koppen_10yr, axis=1)
# df_agg['koppen_15yr'] = df_agg.apply(koppen_15yr, axis=1)
# df_agg['koppen_20yr'] = df_agg.apply(koppen_20yr, axis=1)
# df_agg['koppen_30yr'] = df_agg.apply(koppen_30yr, axis=1)

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
#
# def koppen_name_5yr(s):
#     if (s['koppen_5yr'] == 'Af'):
#         return 'Tropical Rainforest'
#     if (s['koppen_5yr'] == 'Am'):
#         return 'Tropical Monsoon'
#     if (s['koppen_5yr'] == 'Aw'):
#         return 'Savannah'
#     elif (s['koppen_5yr'] == 'BWh'):
#         return 'Hot desert'
#     elif (s['koppen_5yr'] == 'BWk'):
#         return 'Cold desert'
#     elif (s['koppen_5yr'] == 'BSh'):
#         return 'Hot steppe'
#     elif (s['koppen_5yr'] == 'BSk'):
#         return 'Cold steppe'
#     elif (s['koppen_5yr'] == 'Csa'):
#         return 'Mediterranean hot'
#     elif (s['koppen_5yr'] == 'Csb'):
#         return 'Mediterranean warm'
#     elif (s['koppen_5yr'] == 'Csc'):
#         return 'Mediterranean cold'
#     elif (s['koppen_5yr'] == 'Cfa'):
#         return 'Humid subtropical'
#     elif (s['koppen_5yr'] == 'Cfb'):
#         return 'Oceanic'
#     elif (s['koppen_5yr'] == 'Cfc'):
#         return 'Subpolar oceanic'
#     elif (s['koppen_5yr'] == 'Cwa'):
#         return 'Subtropical)'
#     elif (s['koppen_5yr'] == 'Cwb'):
#         return 'Subtropical highland warm'
#     elif (s['koppen_5yr'] == 'Cwc'):
#         return 'Subtropical highland cold'
#     elif (s['koppen_5yr'] == 'Dsa'):
#         return 'Continental (dry summer)'
#     elif (s['koppen_5yr'] == 'Dwa'):
#         return 'Continental (dry winter)'
#     elif (s['koppen_5yr'] == 'Dfa'):
#         return 'Continental (no dry season)'
#     elif (s['koppen_5yr'] == 'Dsb'):
#         return 'Hemiboreal (dry summer)'
#     elif (s['koppen_5yr'] == 'Dwb'):
#         return 'Hemiboreal (dry winter)'
#     elif (s['koppen_5yr'] == 'Dfb'):
#         return 'Hemiboreal (no dry season)'
#     elif (s['koppen_5yr'] == 'Dsc'):
#         return 'Boreal (dry summer)'
#     elif (s['koppen_5yr'] == 'Dwc'):
#         return 'Boreal (dry winter)'
#     elif (s['koppen_5yr'] == 'Dfc'):
#         return 'Boreal (no dry season)'
#     elif (s['koppen_5yr'] == 'Dsd'):
#         return 'Cold Boreal (dry summer)'
#     elif (s['koppen_5yr'] == 'Dwd'):
#         return 'Cold Boreal (dry winter)'
#     elif (s['koppen_5yr'] == 'Dfd'):
#         return 'Cold Boreal (no dry season)'
#     elif (s['koppen_5yr'] == 'EF'):
#         return 'Ice cap'
#     elif (s['koppen_5yr'] == 'ET'):
#         return 'Tundra'
#     elif (s['koppen_5yr'] == 'H'):
#         return 'Highland'
#
# def koppen_name_10yr(s):
#     if (s['koppen_10yr'] == 'Af'):
#         return 'Tropical Rainforest'
#     if (s['koppen_10yr'] == 'Am'):
#         return 'Tropical Monsoon'
#     if (s['koppen_10yr'] == 'Aw'):
#         return 'Savannah'
#     elif (s['koppen_10yr'] == 'BWh'):
#         return 'Hot desert'
#     elif (s['koppen_10yr'] == 'BWk'):
#         return 'Cold desert'
#     elif (s['koppen_10yr'] == 'BSh'):
#         return 'Hot steppe'
#     elif (s['koppen_10yr'] == 'BSk'):
#         return 'Cold steppe'
#     elif (s['koppen_10yr'] == 'Csa'):
#         return 'Mediterranean hot'
#     elif (s['koppen_10yr'] == 'Csb'):
#         return 'Mediterranean warm'
#     elif (s['koppen_10yr'] == 'Csc'):
#         return 'Mediterranean cold'
#     elif (s['koppen_10yr'] == 'Cfa'):
#         return 'Humid subtropical'
#     elif (s['koppen_10yr'] == 'Cfb'):
#         return 'Oceanic'
#     elif (s['koppen_10yr'] == 'Cfc'):
#         return 'Subpolar oceanic'
#     elif (s['koppen_10yr'] == 'Cwa'):
#         return 'Subtropical)'
#     elif (s['koppen_10yr'] == 'Cwb'):
#         return 'Subtropical highland warm'
#     elif (s['koppen_10yr'] == 'Cwc'):
#         return 'Subtropical highland cold'
#     elif (s['koppen_10yr'] == 'Dsa'):
#         return 'Continental (dry summer)'
#     elif (s['koppen_10yr'] == 'Dwa'):
#         return 'Continental (dry winter)'
#     elif (s['koppen_10yr'] == 'Dfa'):
#         return 'Continental (no dry season)'
#     elif (s['koppen_10yr'] == 'Dsb'):
#         return 'Hemiboreal (dry summer)'
#     elif (s['koppen_10yr'] == 'Dwb'):
#         return 'Hemiboreal (dry winter)'
#     elif (s['koppen_10yr'] == 'Dfb'):
#         return 'Hemiboreal (no dry season)'
#     elif (s['koppen_10yr'] == 'Dsc'):
#         return 'Boreal (dry summer)'
#     elif (s['koppen_10yr'] == 'Dwc'):
#         return 'Boreal (dry winter)'
#     elif (s['koppen_10yr'] == 'Dfc'):
#         return 'Boreal (no dry season)'
#     elif (s['koppen_10yr'] == 'Dsd'):
#         return 'Cold Boreal (dry summer)'
#     elif (s['koppen_10yr'] == 'Dwd'):
#         return 'Cold Boreal (dry winter)'
#     elif (s['koppen_10yr'] == 'Dfd'):
#         return 'Cold Boreal (no dry season)'
#     elif (s['koppen_10yr'] == 'EF'):
#         return 'Ice cap'
#     elif (s['koppen_10yr'] == 'ET'):
#         return 'Tundra'
#     elif (s['koppen_10yr'] == 'H'):
#         return 'Highland'
#
# def koppen_name_15yr(s):
#     if (s['koppen_15yr'] == 'Af'):
#         return 'Tropical Rainforest'
#     if (s['koppen_15yr'] == 'Am'):
#         return 'Tropical Monsoon'
#     if (s['koppen_15yr'] == 'Aw'):
#         return 'Savannah'
#     elif (s['koppen_15yr'] == 'BWh'):
#         return 'Hot desert'
#     elif (s['koppen_15yr'] == 'BWk'):
#         return 'Cold desert'
#     elif (s['koppen_15yr'] == 'BSh'):
#         return 'Hot steppe'
#     elif (s['koppen_15yr'] == 'BSk'):
#         return 'Cold steppe'
#     elif (s['koppen_15yr'] == 'Csa'):
#         return 'Mediterranean hot'
#     elif (s['koppen_15yr'] == 'Csb'):
#         return 'Mediterranean warm'
#     elif (s['koppen_15yr'] == 'Csc'):
#         return 'Mediterranean cold'
#     elif (s['koppen_15yr'] == 'Cfa'):
#         return 'Humid subtropical'
#     elif (s['koppen_15yr'] == 'Cfb'):
#         return 'Oceanic'
#     elif (s['koppen_15yr'] == 'Cfc'):
#         return 'Subpolar oceanic'
#     elif (s['koppen_15yr'] == 'Cwa'):
#         return 'Subtropical)'
#     elif (s['koppen_15yr'] == 'Cwb'):
#         return 'Subtropical highland warm'
#     elif (s['koppen_15yr'] == 'Cwc'):
#         return 'Subtropical highland cold'
#     elif (s['koppen_15yr'] == 'Dsa'):
#         return 'Continental (dry summer)'
#     elif (s['koppen_15yr'] == 'Dwa'):
#         return 'Continental (dry winter)'
#     elif (s['koppen_15yr'] == 'Dfa'):
#         return 'Continental (no dry season)'
#     elif (s['koppen_15yr'] == 'Dsb'):
#         return 'Hemiboreal (dry summer)'
#     elif (s['koppen_15yr'] == 'Dwb'):
#         return 'Hemiboreal (dry winter)'
#     elif (s['koppen_15yr'] == 'Dfb'):
#         return 'Hemiboreal (no dry season)'
#     elif (s['koppen_15yr'] == 'Dsc'):
#         return 'Boreal (dry summer)'
#     elif (s['koppen_15yr'] == 'Dwc'):
#         return 'Boreal (dry winter)'
#     elif (s['koppen_15yr'] == 'Dfc'):
#         return 'Boreal (no dry season)'
#     elif (s['koppen_15yr'] == 'Dsd'):
#         return 'Cold Boreal (dry summer)'
#     elif (s['koppen_15yr'] == 'Dwd'):
#         return 'Cold Boreal (dry winter)'
#     elif (s['koppen_15yr'] == 'Dfd'):
#         return 'Cold Boreal (no dry season)'
#     elif (s['koppen_15yr'] == 'EF'):
#         return 'Ice cap'
#     elif (s['koppen_15yr'] == 'ET'):
#         return 'Tundra'
#     elif (s['koppen_15yr'] == 'H'):
#         return 'Highland'
#
# def koppen_name_20yr(s):
#     if (s['koppen_20yr'] == 'Af'):
#         return 'Tropical Rainforest'
#     if (s['koppen_20yr'] == 'Am'):
#         return 'Tropical Monsoon'
#     if (s['koppen_20yr'] == 'Aw'):
#         return 'Savannah'
#     elif (s['koppen_20yr'] == 'BWh'):
#         return 'Hot desert'
#     elif (s['koppen_20yr'] == 'BWk'):
#         return 'Cold desert'
#     elif (s['koppen_20yr'] == 'BSh'):
#         return 'Hot steppe'
#     elif (s['koppen_20yr'] == 'BSk'):
#         return 'Cold steppe'
#     elif (s['koppen_20yr'] == 'Csa'):
#         return 'Mediterranean hot'
#     elif (s['koppen_20yr'] == 'Csb'):
#         return 'Mediterranean warm'
#     elif (s['koppen_20yr'] == 'Csc'):
#         return 'Mediterranean cold'
#     elif (s['koppen_20yr'] == 'Cfa'):
#         return 'Humid subtropical'
#     elif (s['koppen_20yr'] == 'Cfb'):
#         return 'Oceanic'
#     elif (s['koppen_20yr'] == 'Cfc'):
#         return 'Subpolar oceanic'
#     elif (s['koppen_20yr'] == 'Cwa'):
#         return 'Subtropical)'
#     elif (s['koppen_20yr'] == 'Cwb'):
#         return 'Subtropical highland warm'
#     elif (s['koppen_20yr'] == 'Cwc'):
#         return 'Subtropical highland cold'
#     elif (s['koppen_20yr'] == 'Dsa'):
#         return 'Continental (dry summer)'
#     elif (s['koppen_20yr'] == 'Dwa'):
#         return 'Continental (dry winter)'
#     elif (s['koppen_20yr'] == 'Dfa'):
#         return 'Continental (no dry season)'
#     elif (s['koppen_20yr'] == 'Dsb'):
#         return 'Hemiboreal (dry summer)'
#     elif (s['koppen_20yr'] == 'Dwb'):
#         return 'Hemiboreal (dry winter)'
#     elif (s['koppen_20yr'] == 'Dfb'):
#         return 'Hemiboreal (no dry season)'
#     elif (s['koppen_20yr'] == 'Dsc'):
#         return 'Boreal (dry summer)'
#     elif (s['koppen_20yr'] == 'Dwc'):
#         return 'Boreal (dry winter)'
#     elif (s['koppen_20yr'] == 'Dfc'):
#         return 'Boreal (no dry season)'
#     elif (s['koppen_20yr'] == 'Dsd'):
#         return 'Cold Boreal (dry summer)'
#     elif (s['koppen_20yr'] == 'Dwd'):
#         return 'Cold Boreal (dry winter)'
#     elif (s['koppen_20yr'] == 'Dfd'):
#         return 'Cold Boreal (no dry season)'
#     elif (s['koppen_20yr'] == 'EF'):
#         return 'Ice cap'
#     elif (s['koppen_20yr'] == 'ET'):
#         return 'Tundra'
#     elif (s['koppen_20yr'] == 'H'):
#         return 'Highland'
#
# def koppen_name_30yr(s):
#     if (s['koppen_30yr'] == 'Af'):
#         return 'Tropical Rainforest'
#     if (s['koppen_30yr'] == 'Am'):
#         return 'Tropical Monsoon'
#     if (s['koppen_30yr'] == 'Aw'):
#         return 'Savannah'
#     elif (s['koppen_30yr'] == 'BWh'):
#         return 'Hot desert'
#     elif (s['koppen_30yr'] == 'BWk'):
#         return 'Cold desert'
#     elif (s['koppen_30yr'] == 'BSh'):
#         return 'Hot steppe'
#     elif (s['koppen_30yr'] == 'BSk'):
#         return 'Cold steppe'
#     elif (s['koppen_30yr'] == 'Csa'):
#         return 'Mediterranean hot'
#     elif (s['koppen_30yr'] == 'Csb'):
#         return 'Mediterranean warm'
#     elif (s['koppen_30yr'] == 'Csc'):
#         return 'Mediterranean cold'
#     elif (s['koppen_30yr'] == 'Cfa'):
#         return 'Humid subtropical'
#     elif (s['koppen_30yr'] == 'Cfb'):
#         return 'Oceanic'
#     elif (s['koppen_30yr'] == 'Cfc'):
#         return 'Subpolar oceanic'
#     elif (s['koppen_30yr'] == 'Cwa'):
#         return 'Subtropical)'
#     elif (s['koppen_30yr'] == 'Cwb'):
#         return 'Subtropical highland warm'
#     elif (s['koppen_30yr'] == 'Cwc'):
#         return 'Subtropical highland cold'
#     elif (s['koppen_30yr'] == 'Dsa'):
#         return 'Continental (dry summer)'
#     elif (s['koppen_30yr'] == 'Dwa'):
#         return 'Continental (dry winter)'
#     elif (s['koppen_30yr'] == 'Dfa'):
#         return 'Continental (no dry season)'
#     elif (s['koppen_30yr'] == 'Dsb'):
#         return 'Hemiboreal (dry summer)'
#     elif (s['koppen_30yr'] == 'Dwb'):
#         return 'Hemiboreal (dry winter)'
#     elif (s['koppen_30yr'] == 'Dfb'):
#         return 'Hemiboreal (no dry season)'
#     elif (s['koppen_30yr'] == 'Dsc'):
#         return 'Boreal (dry summer)'
#     elif (s['koppen_30yr'] == 'Dwc'):
#         return 'Boreal (dry winter)'
#     elif (s['koppen_30yr'] == 'Dfc'):
#         return 'Boreal (no dry season)'
#     elif (s['koppen_30yr'] == 'Dsd'):
#         return 'Cold Boreal (dry summer)'
#     elif (s['koppen_30yr'] == 'Dwd'):
#         return 'Cold Boreal (dry winter)'
#     elif (s['koppen_30yr'] == 'Dfd'):
#         return 'Cold Boreal (no dry season)'
#     elif (s['koppen_30yr'] == 'EF'):
#         return 'Ice cap'
#     elif (s['koppen_30yr'] == 'ET'):
#         return 'Tundra'
#     elif (s['koppen_30yr'] == 'H'):
#         return 'Highland'

df_agg['koppen_name'] = df_agg.apply(koppen_name, axis=1)
# df_agg['koppen_name_5yr'] = df_agg.apply(koppen_name_5yr, axis=1)
# df_agg['koppen_name_10yr'] = df_agg.apply(koppen_name_10yr, axis=1)
# df_agg['koppen_name_15yr'] = df_agg.apply(koppen_name_15yr, axis=1)
# df_agg['koppen_name_20yr'] = df_agg.apply(koppen_name_20yr, axis=1)
# df_agg['koppen_name_30yr'] = df_agg.apply(koppen_name_30yr, axis=1)

print("assigning K??ppen classes")

# COMMENT OUT IF ADDING TO EXISTING
if os.path.exists('data/koppen_class.csv'):
    os.rename('data/koppen_class.csv', 'data/koppen_class_old.csv')

df_agg.to_csv(df_filename, index=False, quotechar='"', quoting=csv.QUOTE_ALL, header=True)
print("exporting " + df_filename)

# IF EXISTING
# df_agg.to_csv(df_filename, index=False, quotechar='"', mode="a", quoting=csv.QUOTE_ALL, header=False)

# check and de-duplicate
df_check = pd.read_csv(df_filename)
df_check = df_check.drop_duplicates()

df_check.to_csv(df_filename, index=False, quotechar='"', quoting=csv.QUOTE_ALL, header=True)
print("exporting " + df_filename)
