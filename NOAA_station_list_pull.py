#RUN THIS FIRST - probably only need to run once for each batch of states.
#THIS SCRIPT pulls a list of station codes that lie within a given list of geographic boundaries and preps them for intake by NOAA_data_pull.py.  (currently this expects FIPS codes, so states or counties or Census tracts, but it can also be climate regions, countries, or whatever).
#documentation on locations - https://www.ncdc.noaa.gov/cdo-web/webservices/v2#locations

import pandas as pd
import requests
from requests.structures import CaseInsensitiveDict
import json
import csv
import os

token = open('token.key', 'r').read().rstrip('\n')

#TEST CURL to list stations in FIPS - curl -H "token:TOKEN" "https://www.ncdc.noaa.gov/cdo-web/api/v2/stations?locationid=FIPS:53033&startdate=2016-01-01&limit=1000" > ~/Dropbox/EPCW/Projects/Koppen/sample.json

fips_list = open("stations/FIPS_counties_uscontinentalwest.txt", 'r').read().splitlines() #replace file with list of FIPS counties that you're interested in
df_filename = "stations/station_list.csv"
df_listfile = "stations/station_ids.txt"
if os.path.exists('stations/station_list.csv'):
    os.rename('stations/station_list.csv', 'stations/station_list_old.csv')
if os.path.exists('stations/station_ids.txt'):
    os.rename('stations/station_ids.txt', 'stations/station_ids_old.txt')

with open(df_filename, "w") as f:
    f.write('"elevation","mindate","maxdate","latitude","name","datacoverage","id","elevationUnit","longitude"\n')
    f.close()

for fips in fips_list:
    url = "https://www.ncdc.noaa.gov/cdo-web/api/v2/stations?locationid=FIPS:" + fips + "&startdate=2022-01-01&limit=1000"
    headers = CaseInsensitiveDict()
    headers["token"] = token

    resp = requests.get(url, headers=headers)

    r = json.loads(resp.text)
    try:
        df = pd.json_normalize(r["results"]) #json_normalize takes a nested json and makes it a flat table
        df.to_csv(df_filename, mode='a', index=False, quotechar='"', quoting=csv.QUOTE_ALL, header=False)
        df_ids = df[["id"]]
        df_ids.to_csv(df_listfile, mode='a', index=False, header=False)
        print("exporting " + fips)
    except:
        print("skipping " + fips)

#de-duplicate both files
df_cleanids = pd.read_csv(df_listfile)
df_cleanids = df_cleanids.drop_duplicates()
df_cleanids.to_csv(df_listfile, index=False, header=False)
print("cleaning " + df_listfile)

df_cleanlist = pd.read_csv(df_filename)
df_cleanlist = df_cleanlist.drop_duplicates()
df_cleanlist.to_csv(df_filename, index=False, header=True)
print("cleaning " + df_filename)