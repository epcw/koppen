#RUN THIS LAST - only once you have all your data pulled and in /data
#THIS SCRIPT takes data pulled from NOAA_data_pull.py and then assigns a koppen climate code to each station in the dataset for each year that data exists

import pandas as pd
import csv
import os

df_filename = 'data/koppen_class.csv'

df = pd.read_csv('data/station_data.csv')
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
df_temp_agg = df_temp_agg.rename(columns = {'value' : 'temp_coldest_mo'})
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

#bring in the station location data
df_agg = df_agg.merge(df_station_list, how="left", left_on = ['station'], right_on = ['station'])

def koppen(s):
    #run class B first because Arid climates can intersect with the other types, but we want B to take priority over the temp rules
    if (((s['annual_prcp(r)'] < (10 * (2 * s['avg_annual_temp(t)']))) & (s['prcp_winter'] >= (.7 * s['annual_prcp(r)']))) | ((s['annual_prcp(r)'] < (10 * ((2 * s['avg_annual_temp(t)']) + 28))) & (s['prcp_summer'] >= (.7 * s['annual_prcp(r)'] ))) | ((s['annual_prcp(r)'] < (10 * ((2 * s['avg_annual_temp(t)']) + 14))))):
        if (((s['annual_prcp(r)'] < (5 * (2 * s['avg_annual_temp(t)']))) & (s['prcp_winter'] >= (.7 * s['annual_prcp(r)']))) | ((s['annual_prcp(r)'] < (5 * ((2 * s['avg_annual_temp(t)']) + 28))) & (s['prcp_summer'] >= (.7 * s['annual_prcp(r)'] ))) | ((s['annual_prcp(r)'] < (5 * ((2 * s['avg_annual_temp(t)']) + 14))))):
            if(s['avg_annual_temp(t)'] >= 18):
                return 'BWh'
            if (s['avg_annual_temp(t)'] < 18):
                return 'BWk'
        if (((s['annual_prcp(r)'] >= (5 * (2 * s['avg_annual_temp(t)']))) & (s['prcp_winter'] >= (.7 * s['annual_prcp(r)']))) | ((s['annual_prcp(r)'] >= (5 * ((2 * s['avg_annual_temp(t)']) + 28))) & (s['prcp_summer'] >= (.7 * s['annual_prcp(r)']))) | ((s['annual_prcp(r)'] >= (5 * ((2 * s['avg_annual_temp(t)']) + 14))))):
            if(s['avg_annual_temp(t)'] >= 18):
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
    elif (s['temp_hottest_mo'] < 10):
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
        return 'Mediterranean hot (dry summer, hot summer)'
    elif (s['koppen'] == 'Csb'):
        return 'Mediterranean warm (dry summer, warm summer)'
    elif (s['koppen'] == 'Csc'):
        return 'Mediterranean cold (dry summer, cold summer)'
    elif (s['koppen'] == 'Cfa'):
        return 'Humid subtropical (no dry season, hot summer)'
    elif (s['koppen'] == 'Cfb'):
        return 'Oceanic (no dry season, warm summer)'
    elif (s['koppen'] == 'Cfc'):
        return 'Subpolar oceanic (no dry season, cold summer)'
    elif (s['koppen'] == 'Cwa'):
        return 'Subtropical (Dry winter, hot summer)'
    elif (s['koppen'] == 'Cwb'):
        return 'Subtropical highland (dry winter, warm summer)'
    elif (s['koppen'] == 'Cwc'):
        return 'Subtropical highland (dry winter, cold summer)'
    elif (s['koppen'] == 'Dsa'):
        return 'Continental (dry winter, hot summer)'
    elif (s['koppen'] == 'Dwa'):
        return 'Continental (dry summer, hot summer)'
    elif (s['koppen'] == 'Dfa'):
        return 'Continental (no dry season, hot summer)'
    elif (s['koppen'] == 'Dsb'):
        return 'Hemiboreal (dry summer, warm summer)'
    elif (s['koppen'] == 'Dwb'):
        return 'Hemiboreal (dry winter, warm summer)'
    elif (s['koppen'] == 'Dfb'):
        return 'Hemiboreal (no dry season, warm summer)'
    elif (s['koppen'] == 'Dsc'):
        return 'Boreal (dry summer, cold summer)'
    elif (s['koppen'] == 'Dwc'):
        return 'Boreal (dry winter, cold summer)'
    elif (s['koppen'] == 'Dfc'):
        return 'Boreal (no dry season, cold summer)'
    elif (s['koppen'] == 'Dsd'):
        return 'Boreal (dry summer, severe winters)'
    elif (s['koppen'] == 'Dwd'):
        return 'Boreal (dry winter, severe winters)'
    elif (s['koppen'] == 'Dfd'):
        return 'Boreal (no dry season, severe winters)'
    elif (s['koppen'] == 'EF'):
        return 'Ice cap'
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

#IF EXISTING
df_agg.to_csv(df_filename, index=False, quotechar='"', mode="a", quoting=csv.QUOTE_ALL, header=False)
print("exporting " + df_filename)