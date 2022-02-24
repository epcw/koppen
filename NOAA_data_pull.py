import pandas as pd
import requests
from requests.structures import CaseInsensitiveDict
import json
import csv
import os

token = open('token.key', 'r').read().rstrip('\n')

#TEST CURL for station data - curl -H "token:token" "https://www.ncdc.noaa.gov/cdo-web/api/v2/data?station=GHCN:US1WAKG0188&datasetid=GSOM&units=metric&includeStationLocation=true&includeStationName=true&startdate=1960-01-01&enddate=1960-12-31&datatypeid=TAVG,PRCP" > ~/Dropbox/EPCW/Projects/Koppen/sample.json
#TEST CURL for active station - curl -H "token:token" "https://www.ncdc.noaa.gov/cdo-web/api/v2/stations/GHCN:US1WAKG0188/?units=metric&includeStationLocation=true&includeStationName=true&startdate=2012-01-01&enddate=2021-12-31" > ~/Dropbox/EPCW/Projects/Koppen/sample.json
#data codes - https://www1.ncdc.noaa.gov/pub/data/ghcn/daily/readme.txt

station_list = open("stations/station_ids.txt", 'r').read().splitlines()
df_filename = "data/station_data.csv"
if os.path.exists('data/station_data.csv'):
    os.rename('data/station_data.csv', 'data/station_data_old.csv')
with open(df_filename, "w") as f:
    f.write('"date","datatype","station","attributes","value"\n')
    f.close()

dates = list(range(1982,2022)) #reminder: end date will be one year less than end of range

for s in station_list: #iterate over station list
    for d in dates: #iterate over date range (have to chunk this because there is the 1000 record limit per pull)
        url = "https://www.ncdc.noaa.gov/cdo-web/api/v2/data?datasetid=GSOM&stationid=" + s + "&startdate=" + str(d) + "-01-01&enddate=" + str(d) + "-12-31&limit=1000&units=metric&datatypeid=PRCP,TAVG" #NOTE 10 years at a time max
        headers = CaseInsensitiveDict()
        headers["token"] = token

        resp = requests.get(url, headers=headers)
        r = json.loads(resp.text) #don't stick this inside the try because when it fails, you know you've run out of requests for the day (10k/day)
        try:
            df = pd.json_normalize(r["results"]) #json_normalize takes a nested json and makes it a flat table
            df.to_csv(df_filename, mode='a', index=False, quotechar='"', quoting=csv.QUOTE_ALL, header=False)
            print("exporting " + s + " " + str(d))
        except:
            print("skipping " + s + " " + str(d)) #handle stations that are not available for the entire length of the pull
