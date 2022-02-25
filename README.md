# Center for Equitable Policy in a Changing World
##The Changing Köppen Climate of the US West

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

### Data source
National Oceanographic and Atmospheric Administration [Climate Data Online API](https://www.ncdc.noaa.gov/cdo-web/webservices/v2)\
Dataset: Global Summary of the Month (GSOM), 1982-2021 monthly total PRCP & avg temp.

### Principal researchers
Richard W. Sharp\
Patrick W. Zimmerman

#### Codebase
**Data prep**: Python 3.8\
**Webpage**: HTML & CSS (_planned_)\
**Visualization**: d3.v5, with jQuery 3.5.1 to selectively color (_planned_)

#### Python Package requirements (as well as all their dependencies)
csv\
json\
os\
pandas\
requests

#### Other requirements
Webserver (_planned_)