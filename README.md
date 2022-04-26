# Center for Equitable Policy in a Changing World
## The Changing Köppen Climate of the US West

### Project Description:
This is an initial attempt to visualize the changing climate in the American West by using NWS data to define changing Köppen classifications over the past 40 years.

#### Instructions
Order of scripts:
1. _NOAA_station_list_pull.py_ (skip if you already have a list of NOAA station codes handy)
   - Compiles a list of stations within a given set of geographic areas (FIPS, ZIP, countries, etc.)
2. _NOAA_data_pull.py_
   - Pulls weather data from the stations in the list above (precipitation, wind, temp, etc.)
3. _koppen.py_
   - Assigns a koppen classification to each station in the above list for each year of data.
4. _data-prep.py_
   - Takes koppen_data.csv and creates map-readable station and edges files.
5. _upload files to server_
   - root folder:
     - index.html
   - /map:
     - edges.csv
     - jquery.3.5.1.min.js
     - koppen.css
     - koppen.js
     - states-albers-10m.json
     - stations.csv

**Important Note** - For any medium-to-large scripts, you'll need to run steps 2-5 multiple times.  For every time after the first, you will need to comment out the noted areas in NOAA_data_pull.py & koppen.py so that the scripts add to the existing data files (data/station_data.csv & data/koppen_data.csv) rather than overwrite them. 

### Data source
National Oceanographic and Atmospheric Administration [Climate Data Online API](https://www.ncdc.noaa.gov/cdo-web/webservices/v2)\
Dataset: Global Summary of the Month (GSOM), 1982-2021 monthly total PRCP & avg temp.
Koppen class definitions derived from [Beck, H., Zimmermann, N., McVicar, T. et al. Present and future Köppen-Geiger climate classification maps at 1-km resolution. Sci Data 5, 180214 (2018). https://doi.org/10.1038/sdata.2018.214](https://www.nature.com/articles/sdata2018214)

### Map Shapefiles
[US Atlas TopoJSON](https://github.com/topojson/us-atlas). ObservableHQ's redistribution of the US Census Bureau’s cartographic boundary shapefiles, 2017 edition. Specifically, [states-albers-10m.json](https://cdn.jsdelivr.net/npm/us-atlas@3/states-albers-10m.json)

### Principal researchers
Richard W. Sharp\
Patrick W. Zimmerman

#### Codebase
**Data prep**: Python 3.8\
**Webpage**: HTML & CSS\
**Visualization**: d3.v5, with jQuery 3.5.1 to selectively color

#### Python Package requirements (as well as all their dependencies)
csv\
json\
os\
pandas\
requests

#### Other requirements
Webserver