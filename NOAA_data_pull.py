#RUN THIS SECOND - after you have a list of station codes (expecting one code per line)
#THIS SCRIPT pulls weather data for a given list of station codes (see NOAA_station_list_pull.py).  It's currently set to take total precipitation (PRCP) and average temp (TAVG) from the Global Summary of the Month dataset (GSOM).
#documentation on how to use other datasets & measurements - https://www.ncdc.noaa.gov/cdo-web/webservices/v2
#data codes - https://www1.ncdc.noaa.gov/pub/data/ghcn/daily/readme.txt

import pandas as pd
import requests
from requests.structures import CaseInsensitiveDict
import json
import csv
import os
import sys

token = open('token.key', 'r').read().rstrip('\n')

#TEST CURL for station data - curl -H "token:token" "https://www.ncdc.noaa.gov/cdo-web/api/v2/data?station=GHCN:US1WAKG0188&datasetid=GSOM&units=metric&includeStationLocation=true&includeStationName=true&startdate=1960-01-01&enddate=1960-12-31&datatypeid=TAVG,PRCP" > ~/Dropbox/EPCW/Projects/Koppen/sample.json
#TEST CURL for active station - curl -H "token:token" "https://www.ncdc.noaa.gov/cdo-web/api/v2/stations/GHCN:US1WAKG0188/?units=metric&includeStationLocation=true&includeStationName=true&startdate=2012-01-01&enddate=2021-12-31" > ~/Dropbox/EPCW/Projects/Koppen/sample.json


station_list = open("stations/station_ids.txt", 'r').read().splitlines() #if you've had to start / stop the script, make sure to save a station_ids-COMPLETE.txt file
df_filename = "data/station_data.csv"

#comment out if you're running a repeat pull (as in you need to stop and restart the script)
# if os.path.exists('data/station_data.csv'):
#     os.rename('data/station_data.csv', 'data/station_data_old.csv')
# with open(df_filename, "w") as f:
#     f.write('"date","datatype","station","attributes","value"\n')
#     f.close()

dates = list(range(1982,2022)) #reminder: end date will be one year less than end of range

for s in station_list: #iterate over station list
    for d in dates: #iterate over date range (have to chunk this because there is the 1000 record limit per pull)
        url = "https://www.ncdc.noaa.gov/cdo-web/api/v2/data?datasetid=GSOM&stationid=" + s + "&startdate=" + str(d) + "-01-01&enddate=" + str(d) + "-12-31&limit=1000&units=metric&datatypeid=PRCP,TAVG" #NOTE 10 years at a time max
        headers = CaseInsensitiveDict()
        headers["token"] = token

        resp = requests.get(url, headers=headers)
        if resp.status_code == 429:
            failfile = "failfile.txt"
            failstring = "Status code 429. Limit reached on: " + s + " " + str(d)
            with open(failfile, 'w') as ff:
                ff.write(failstring)
                ff.close()
            sys.exit("limit reached")
        elif resp.status_code == 200:
            r = json.loads(resp.text)  # don't stick this inside the try because when it fails, you know you've run out of requests for the day (10k/day)
            try:
                df = pd.json_normalize(r["results"]) #json_normalize takes a nested json and makes it a flat table
                df.to_csv(df_filename, mode='a', index=False, quotechar='"', quoting=csv.QUOTE_ALL, header=False)
                print("exporting " + s + " " + str(d))
            except:
                print("skipping " + s + " " + str(d)) #handle stations that are not available for the entire length of the pull
        elif resp.status_code == 502:
            print("bad gateway (502) on " + s + " " + str(d) + ". Retrying.")
            resp = requests.get(url, headers=headers)
            r = json.loads(resp.text)  # don't stick this inside the try because when it fails, you know you've run out of requests for the day (10k/day)
            try:
                df = pd.json_normalize(r["results"]) #json_normalize takes a nested json and makes it a flat table
                df.to_csv(df_filename, mode='a', index=False, quotechar='"', quoting=csv.QUOTE_ALL, header=False)
                print("exporting " + s + " " + str(d))
            except:
                failfile = "failfile.txt"
                failstring = str(resp.status_code) + " on: " + s + " " + str(d)
                with open(failfile, 'w') as ff:
                    ff.write(failstring)
                    ff.close()
                sys.exit(resp.status_code)
        else:
            failfile = "failfile.txt"
            failstring = str(resp.status_code) + " on: " + s + " " + str(d)
            with open(failfile, 'w') as ff:
                ff.write(failstring)
                ff.close()
            sys.exit(resp.status_code)