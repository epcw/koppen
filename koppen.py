# RUN THIS LAST - only once you have all your data pulled and in /data
# THIS SCRIPT takes data pulled from NOAA_data_pull.py and then assigns a koppen climate code to each station in the dataset for each year that data exists

import pandas as pd
import csv
import os
from scipy import stats

df_filename = 'data/koppen_class.csv'

df = pd.read_csv('data/station_data.csv')
df['date'] = pd.to_datetime(df['date'])  # converts the date string to datetime

df_station_list = pd.read_csv('stations/station_list_complete.csv')
df_station_list = df_station_list[['elevation', 'mindate', 'maxdate', 'latitude', 'longitude', 'name', 'id']]
df_station_list = df_station_list.rename(columns={'id': 'station'})
df_station_list['elevation'] = pd.to_numeric(df_station_list.elevation, errors='coerce') #this accounts for some wacko cells in the elevation column - probably string NaNs or "N/A" or "unknown" or some other manual notes written in grease pen from a surveyor in like 1943.

df = df.merge(df_station_list, how='inner', left_on=['station'], right_on=['station'])

winter_months = [1, 2, 3, 10, 11, 12]  # from Koppen climate classification for N hemisphere
df['year_half'] = 'summer'
df.loc[df['date'].dt.month.isin(winter_months), 'year_half'] = 'winter'

df['year'] = df['date'].dt.year
# years_to_avg = [5, 10, 15, 20, 25, 30] # don't bother to include the 1 since you don't have to calc this as a rolling function POSSIBLY NAME TIMESCALE FOR PEOPLE WHO AREN'T ME AND RICH TO KNOW WHAT THE FUCK THIS IS

# calculate temperature metrics per station per year
df_temp = df[(df['datatype'] == 'TAVG')]
df_temp = df_temp[['date', 'year', 'datatype', 'station', 'value']]

df_temp = df_temp.drop_duplicates()

df_temp['num_mo_btw_10-22C'] = 0
df_temp.loc[(df_temp['value'] > 10) & (df_temp['value'] < 22), 'num_mo_btw_10-22C'] = 1
df_agg = df_temp.groupby(['station', 'year'])[['num_mo_btw_10-22C']].sum().reset_index()
df_agg['years_averaged'] = 1
df_agg['num_mo_btw_10-22C'] = df_agg['num_mo_btw_10-22C'].astype('float64')
df_agg['year'] = df_agg['year'].astype('int')
df_agg['years_averaged'] = df_agg['years_averaged'].astype('int')

####
df_temp_agg = df_temp.groupby(['station', 'year'])[['value']].min().reset_index()
df_temp_agg = df_temp_agg.rename(columns={'value': 'temp_coldest_mo'})
df_temp_agg['years_averaged'] = 1
df_coldest = df_temp_agg

# for y in years_to_avg:
#     df_temp_agg_temp = df_temp_agg.groupby(['station']).rolling(y,y, center=True,on='year').mean().reset_index()
#     df_temp_agg_temp = df_temp_agg_temp.drop(columns="level_1")
#     df_temp_agg_temp['years_averaged'] = y
#     df_coldest = pd.concat([df_coldest,df_temp_agg_temp],axis=0).drop_duplicates()

df_agg = df_agg.merge(df_coldest, how="outer",  left_on=['station', 'year','years_averaged'], right_on=['station', 'year','years_averaged']).drop_duplicates()

df_temp_agg = df_temp.groupby(['station', 'year'])[['value']].max().reset_index()
df_temp_agg = df_temp_agg.rename(columns={'value': 'temp_hottest_mo'})
df_temp_agg['years_averaged'] = 1
df_hottest = df_temp_agg

# for y in years_to_avg:
#     df_temp_agg_temp = df_temp_agg.groupby(['station']).rolling(y,y, center=True,on='year').mean().reset_index()
#     df_temp_agg_temp = df_temp_agg_temp.drop(columns="level_1")
#     df_temp_agg_temp['years_averaged'] = y
#     df_hottest = pd.concat([df_hottest,df_temp_agg_temp],axis=0).drop_duplicates()

df_agg = df_agg.merge(df_hottest, how="outer",  left_on=['station', 'year','years_averaged'], right_on=['station', 'year','years_averaged']).drop_duplicates()

df_temp_agg = df_temp.groupby(['station', 'year'])[['value']].mean().reset_index()
df_temp_agg = df_temp_agg.rename(columns={'value': 'avg_annual_temp(t)'})
df_temp_agg['years_averaged'] = 1
df_mean = df_temp_agg

# for y in years_to_avg:
#     df_temp_agg_temp = df_temp_agg.groupby(['station']).rolling(y,y, center=True,on='year').mean().reset_index()
#     df_temp_agg_temp = df_temp_agg_temp.drop(columns="level_1")
#     df_temp_agg_temp['years_averaged'] = y
#     df_mean = pd.concat([df_mean,df_temp_agg_temp],axis=0).drop_duplicates()

df_agg = df_agg.merge(df_mean, how="outer",  left_on=['station', 'year','years_averaged'], right_on=['station', 'year','years_averaged']).drop_duplicates()

####

########
# # calculate precipitation metrics per station per year
df_prcp = df[(df['datatype'] == 'PRCP')]
df_prcp = df_prcp[['date', 'year', 'datatype', 'station', 'value', 'year_half']]
df_prcp = df_prcp.drop_duplicates()
df_prcp_agg = df_prcp.groupby(['station', 'year'])[['value']].min().reset_index()
df_prcp_agg = df_prcp_agg.rename(columns={'value': 'prcp_driest_mo'})
df_prcp_agg['years_averaged'] = 1
df_prcp_agg['years_averaged'] = df_prcp_agg['years_averaged'].astype('int')
df_driest = df_prcp_agg

# for y in years_to_avg:
#     df_prcp_agg_temp = df_prcp_agg.groupby(['station']).rolling(y,y, center=True,on='year').mean().reset_index()
#     df_prcp_agg_temp = df_prcp_agg_temp.drop(columns="level_1")
#     df_prcp_agg_temp['years_averaged'] = y
#     df_driest = pd.concat([df_driest,df_prcp_agg_temp],axis=0).drop_duplicates()

df_agg = df_agg.merge(df_driest, how="outer",  left_on=['station', 'year','years_averaged'], right_on=['station', 'year','years_averaged']).drop_duplicates()

df_prcp_agg = df_prcp.groupby(['station', 'year'])[['value']].max().reset_index()
df_prcp_agg = df_prcp_agg.rename(columns={'value': 'prcp_wettest_mo'})
df_prcp_agg['years_averaged'] = 1
df_prcp_agg['years_averaged'] = df_prcp_agg['years_averaged'].astype('int')
df_wettest = df_prcp_agg

# for y in years_to_avg:
#     df_prcp_agg_temp = df_prcp_agg.groupby(['station']).rolling(y,y, center=True,on='year').mean().reset_index()
#     df_prcp_agg_temp = df_prcp_agg_temp.drop(columns="level_1")
#     df_prcp_agg_temp['years_averaged'] = y
#     df_wettest = pd.concat([df_wettest,df_prcp_agg_temp],axis=0).drop_duplicates()

df_agg = df_agg.merge(df_wettest, how="outer",  left_on=['station', 'year','years_averaged'], right_on=['station', 'year','years_averaged']).drop_duplicates()

df_prcp_agg = df_prcp.groupby(['station', 'year'])[['value']].sum().reset_index()
df_prcp_agg = df_prcp_agg.rename(columns={'value': 'annual_prcp(r)'})
df_prcp_agg['years_averaged'] = 1
df_prcp_agg['years_averaged'] = df_prcp_agg['years_averaged'].astype('int')
df_tot = df_prcp_agg

# for y in years_to_avg:
#     df_prcp_agg_temp = df_prcp_agg.groupby(['station']).rolling(y,y, center=True,on='year').mean().reset_index()
#     df_prcp_agg_temp = df_prcp_agg_temp.drop(columns="level_1")
#     df_prcp_agg_temp['years_averaged'] = y
#     df_tot = pd.concat([df_tot,df_prcp_agg_temp],axis=0).drop_duplicates()

df_agg = df_agg.merge(df_tot, how="outer",  left_on=['station', 'year','years_averaged'], right_on=['station', 'year','years_averaged']).drop_duplicates()

df_prcp_summer = df_prcp[(df_prcp['year_half'] == 'summer')]

df_prcp_summer_agg = df_prcp_summer.groupby(['station', 'year'])[['value']].sum().reset_index()
df_prcp_summer_agg = df_prcp_summer_agg.rename(columns={'value': 'prcp_summer'})
df_prcp_summer_agg['years_averaged'] = 1
df_prcp_summer_agg['years_averaged'] = df_prcp_agg['years_averaged'].astype('int')
df_sum_tot = df_prcp_summer_agg

# for y in years_to_avg:
#     df_prcp_summer_agg_temp = df_prcp_summer_agg.groupby(['station']).rolling(y,y, center=True,on='year').mean().reset_index()
#     df_prcp_summer_agg_temp = df_prcp_summer_agg_temp.drop(columns="level_1")
#     df_prcp_summer_agg_temp['years_averaged'] = y
#     df_sum_tot = pd.concat([df_sum_tot,df_prcp_summer_agg_temp],axis=0).drop_duplicates()

df_agg = df_agg.merge(df_sum_tot, how="outer",  left_on=['station', 'year','years_averaged'], right_on=['station', 'year','years_averaged']).drop_duplicates()

df_prcp_summer_agg = df_prcp_summer.groupby(['station', 'year'])[['value']].min().reset_index()
df_prcp_summer_agg = df_prcp_summer_agg.rename(columns={'value': 'prcp_driest_mo_summer'})
df_prcp_summer_agg['years_averaged'] = 1
df_prcp_summer_agg['years_averaged'] = df_prcp_agg['years_averaged'].astype('int')
df_sum_dry = df_prcp_summer_agg

# for y in years_to_avg:
#     df_prcp_summer_agg_temp = df_prcp_summer_agg.groupby(['station']).rolling(y,y, center=True,on='year').mean().reset_index()
#     df_prcp_summer_agg_temp = df_prcp_summer_agg_temp.drop(columns="level_1")
#     df_prcp_summer_agg_temp['years_averaged'] = y
#     df_sum_dry = pd.concat([df_sum_dry,df_prcp_summer_agg_temp],axis=0).drop_duplicates()

df_agg = df_agg.merge(df_sum_dry, how="outer",  left_on=['station', 'year','years_averaged'], right_on=['station', 'year','years_averaged']).drop_duplicates()

df_prcp_summer_agg = df_prcp_summer.groupby(['station', 'year'])[['value']].max().reset_index()
df_prcp_summer_agg = df_prcp_summer_agg.rename(columns={'value': 'prcp_wettest_mo_summer'})
df_prcp_summer_agg['years_averaged'] = 1
df_prcp_summer_agg['years_averaged'] = df_prcp_agg['years_averaged'].astype('int')
df_sum_wet = df_prcp_summer_agg

# for y in years_to_avg:
#     df_prcp_summer_agg_temp = df_prcp_summer_agg.groupby(['station']).rolling(y,y, center=True,on='year').mean().reset_index()
#     df_prcp_summer_agg_temp = df_prcp_summer_agg_temp.drop(columns="level_1")
#     df_prcp_summer_agg_temp['years_averaged'] = y
#     df_sum_wet = pd.concat([df_sum_wet,df_prcp_summer_agg_temp],axis=0).drop_duplicates()

df_agg = df_agg.merge(df_sum_wet, how="outer",  left_on=['station', 'year','years_averaged'], right_on=['station', 'year','years_averaged']).drop_duplicates()

df_prcp_winter = df_prcp[(df_prcp['year_half'] == 'winter')]

df_prcp_winter_agg = df_prcp_winter.groupby(['station', 'year'])[['value']].sum().reset_index()
df_prcp_winter_agg = df_prcp_winter_agg.rename(columns={'value': 'prcp_winter'})
df_prcp_winter_agg['years_averaged'] = 1
df_prcp_winter_agg['years_averaged'] = df_prcp_agg['years_averaged'].astype('int')
df_win_tot = df_prcp_winter_agg

# for y in years_to_avg:
#     df_prcp_winter_agg_temp = df_prcp_winter_agg.groupby(['station']).rolling(y,y, center=True,on='year').mean().reset_index()
#     df_prcp_winter_agg_temp = df_prcp_winter_agg_temp.drop(columns="level_1")
#     df_prcp_winter_agg_temp['years_averaged'] = y
#     df_win_tot = pd.concat([df_win_tot,df_prcp_winter_agg_temp],axis=0).drop_duplicates()

df_agg = df_agg.merge(df_win_tot, how="outer",  left_on=['station', 'year','years_averaged'], right_on=['station', 'year','years_averaged']).drop_duplicates()

df_prcp_winter_agg = df_prcp_winter.groupby(['station', 'year'])[['value']].min().reset_index()
df_prcp_winter_agg = df_prcp_winter_agg.rename(columns={'value': 'prcp_driest_mo_winter'})
df_prcp_winter_agg['years_averaged'] = 1
df_prcp_winter_agg['years_averaged'] = df_prcp_agg['years_averaged'].astype('int')
df_win_dry = df_prcp_winter_agg

# for y in years_to_avg:
#     df_prcp_winter_agg_temp = df_prcp_winter_agg.groupby(['station']).rolling(y,y, center=True,on='year').mean().reset_index()
#     df_prcp_winter_agg_temp = df_prcp_winter_agg_temp.drop(columns="level_1")
#     df_prcp_winter_agg_temp['years_averaged'] = y
#     df_win_dry = pd.concat([df_win_dry,df_prcp_winter_agg_temp],axis=0).drop_duplicates()

df_agg = df_agg.merge(df_win_dry, how="outer",  left_on=['station', 'year','years_averaged'], right_on=['station', 'year','years_averaged']).drop_duplicates()

df_prcp_winter_agg = df_prcp_winter.groupby(['station', 'year'])[['value']].max().reset_index()
df_prcp_winter_agg = df_prcp_winter_agg.rename(columns={'value': 'prcp_wettest_mo_winter'})
df_prcp_winter_agg['years_averaged'] = 1
df_prcp_winter_agg['years_averaged'] = df_prcp_agg['years_averaged'].astype('int')
df_win_wet = df_prcp_winter_agg

# for y in years_to_avg:
#     df_prcp_winter_agg_temp = df_prcp_winter_agg.groupby(['station']).rolling(y,y, center=True,on='year').mean().reset_index()
#     df_prcp_winter_agg_temp = df_prcp_winter_agg_temp.drop(columns="level_1")
#     df_prcp_winter_agg_temp['years_averaged'] = y
#     df_win_wet = pd.concat([df_win_wet,df_prcp_winter_agg_temp],axis=0).drop_duplicates()

df_agg = df_agg.merge(df_win_wet, how="outer",  left_on=['station', 'year','years_averaged'], right_on=['station', 'year','years_averaged']).drop_duplicates()


########

df_temp_agg = df_agg
#
# stations = {
#     'SF downtown': 'GHCND:USW00023272',
#     'SEA':         'GHCND:USW00024233',
#     'SFO':         'GHCND:USW00023234'
# }

target_cols = [ c for c in df_temp_agg.columns if c not in ['station', 'year', 'years_averaged'] ]
#
# target_col = target_cols[0]

target_col = target_cols[6]

n_years = 30
roll_avg_suffix = f'_{n_years}yr_roll_avg'
roll_avg_col = f'{target_col}{roll_avg_suffix}'

new_df = df_temp_agg.groupby(['station']).rolling(1, on='year')[target_col].mean()
roll_avg_df = df_temp_agg.merge(new_df.to_frame().reset_index(),
                                how='inner',
                                right_on=['station', 'year'],
                                left_on=['station', 'year'],
                                suffixes=('', roll_avg_suffix)
                                )


new_df = df_temp_agg.groupby(['station']).rolling(n_years, on='year', center=False)[target_col].mean()
roll_avg_df = df_temp_agg.merge(new_df.to_frame().reset_index(),
                                how='inner',
                                right_on=['station', 'year'],
                                left_on=['station', 'year'],
                                suffixes=('', roll_avg_suffix)
                                )


new_df = df_temp_agg.groupby(['station']).rolling(n_years, on='year', center=True)[target_col].mean()
roll_avg_df = df_temp_agg.merge(new_df.to_frame().reset_index(),
                                how='inner',
                                right_on=['station', 'year'],
                                left_on=['station', 'year'],
                                suffixes=('', roll_avg_suffix)
                                )


def station_result(station_name, station, target_cols, df_temp_agg, n_years=30, min_year=1992, max_year=2025,
                   min_cutoff_year=1950):
    station_filter = (df_temp_agg['station'] == station)

    df_result = None

    for target_col in target_cols:
        ##########################
        ## standard moving average
        ##########################
        roll_avg_suffix = f'_{n_years}yr_roll_avg_standard'
        roll_avg_col_standard = f'{target_col}{roll_avg_suffix}'

        new_df = df_temp_agg[station_filter].groupby(['station']).rolling(n_years, on='year', center=False)[
            target_col].mean()
        roll_avg_df = df_temp_agg.merge(new_df.to_frame().reset_index(),
                                        how='inner',
                                        right_on=['station', 'year'],
                                        left_on=['station', 'year'],
                                        suffixes=('', roll_avg_suffix)
                                        )

        combo = roll_avg_df[['station', 'year', target_col]]

        right = roll_avg_df[roll_avg_df['station'] == station][['station', 'year', roll_avg_col_standard]]
        combo = combo.merge(right, on=['station', 'year'], how='left')

        ##########################
        ## centered moving average
        ##########################

        roll_avg_suffix = f'_{n_years}yr_roll_avg_centered'
        roll_avg_col = f'{target_col}{roll_avg_suffix}'

        new_df = df_temp_agg[station_filter].groupby(['station']).rolling(n_years, on='year', center=True)[
            target_col].mean()
        roll_avg_df = df_temp_agg.merge(new_df.to_frame().reset_index(),
                                        how='inner',
                                        right_on=['station', 'year'],
                                        left_on=['station', 'year'],
                                        suffixes=('', roll_avg_suffix)
                                        )

        # combo = roll_avg_df[['station', 'year', target_col]]

        right = roll_avg_df[roll_avg_df['station'] == station][['station', 'year', roll_avg_col]]
        combo = combo.merge(right, on=['station', 'year'], how='left')

        ######################################
        ## Linear regression for extrapolation
        ######################################

        data = roll_avg_df[
            (roll_avg_df['station'] == station) & \
            (roll_avg_df['years_averaged'] == 1) & \
            (roll_avg_df[roll_avg_col].notnull())
            ].drop_duplicates()

        x = data[data['year'] >= min_year]['year']
        y = data[data['year'] >= min_year][roll_avg_col]

        lr = stats.linregress(x, y)
        m = lr.slope
        b = lr.intercept
        x_int = [year for year in range(min(x), max_year)]
        y_int = [m * xi + b for xi in x_int]

        df_int = pd.DataFrame({
            'station': [station for i in range(len(x_int))],
            'year': x_int,
            roll_avg_col + '_lr': y_int
        })

        right = df_int
        combo = combo.merge(right, on=['station', 'year'], how='left')

        combo[roll_avg_col + '_combo'] = combo[roll_avg_col].fillna(combo[roll_avg_col + '_lr'])

        ##################
        ## assemble result
        ##################

        df_a = combo[['station', 'year', target_col]].copy()
        df_a['tag'] = '1yr'

        df_b = combo[['station', 'year', roll_avg_col + '_combo']].copy()
        df_b = df_b.rename(columns={roll_avg_col + '_combo': target_col})
        df_b['tag'] = 'LR'

        df_c = combo[['station', 'year', roll_avg_col_standard]].copy()
        df_c = df_c.rename(columns={roll_avg_col_standard: target_col})
        df_c['tag'] = 'LAG'

        df_d = pd.concat([df_b, df_c])

        if df_result is None:
            df_result = df_d[['station', 'year', 'tag', target_col]]
        else:
            df_result = df_result.merge(df_d[['station', 'year', 'tag', target_col]],
                                        how='outer',
                                        on=['station', 'year', 'tag'])

        df_result_filtered = df_result[df_result['year'] >= min_cutoff_year].copy()

    return df_result_filtered


station_dfs = list()
ctr_yay = 0
ctr_oops = 0

# for station_name, station in stations.items():
for i, r in df_station_list.iterrows():
    station_name = r['name']
    station = r['station']
    try:
        station_dfs.append(station_result(station_name, station, target_cols, df_temp_agg))
        ctr_yay += 1
    except:
        ctr_oops += 1
        print(f'YAY: {ctr_yay} | OOOPS: {ctr_oops} | {station}')
        continue

koppen_df = pd.concat(station_dfs)

koppen_df = koppen_df.merge(df_station_list, how='inner', left_on=['station'], right_on=['station'])

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

print("assigning Köppen classes")

koppen_df['koppen'] = koppen_df.apply(koppen, axis=1)

koppen_df['koppen_name'] = koppen_df.apply(koppen_name, axis=1)

# COMMENT OUT IF ADDING TO EXISTING
if os.path.exists('data/koppen_class.csv'):
    os.rename('data/koppen_class.csv', 'data/koppen_class_old.csv')

print("exporting " + df_filename)

koppen_df.to_csv(df_filename,
                 header=True,
                 index=False,
                 quotechar='"',
                 quoting=csv.QUOTE_ALL)
