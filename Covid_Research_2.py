# Dylan DeWolfe
# Covid Research Final Project
# Covid_Research.py
# CPSC 408

import pandas as pd
import numpy as np
import requests
import lxml
import csv 
from pandas import DataFrame
import mysql.connector
import sqlalchemy
from wwo_hist import retrieve_hist_data
# -----------------------------------------------------------------------

# should really make a sep. class for the connection for security purposes.
# using mysql connector
my_db = mysql.connector.connect(
  host="34.106.102.239",
  user="root",
  password="2421",
  database="auto_covid_data"
)

engine = sqlalchemy.create_engine("mysql+mysqlconnector://{user}:{pw}@34.106.102.239/{db}"
                       .format(user="root",
                               pw="2421",
                               db="auto_covid_data"))

cursor = my_db.cursor()
# -----------------------------------------------------------------------
# importing the data
# usa_df = cases and deaths automatically updating, per state and county, with lat and long.
usa_df = pd.read_csv('https://raw.githubusercontent.com/imdevskp/covid_19_jhu_data_web_scrap_and_cleaning/master/usa_county_wise.csv')
# ------------------------------------------------------------------------
url = 'https://www.worldometers.info/coronavirus/country/us/'

header = {
  "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.75 Safari/537.36",
  "X-Requested-With": "XMLHttpRequest"
}

r = requests.get(url, headers=header)
# pop_df includes population data (deahts/1m pop and tot cases/1m pop) , new cases, active cases, tests

pop_df = pd.read_html(r.text)[0]
print(pop_df.head())
# need to drop source and projections columns.... Also need to drop new cases and new deaths..
pop_df = pop_df.drop(['Source', 'Projections', 'NewCases', 'NewDeaths'], axis = 1)
# -----------------------------------------------------------------------
# need to subset the data further
# subset by state, lat and long, and date
# -----------------------------------------------------------------------

usa_latest = usa_df[usa_df['Date'] == max(usa_df['Date'])]
usa_map_df = usa_df[usa_df['Date'] == max(usa_df['Date'])]
# -----------------------------------------------------------------------
# grouping data 58 total states (including us territories)
states_grouped = usa_latest.groupby('Province_State')['Confirmed', 'Deaths'].sum().reset_index()
print(states_grouped)
# -----------------------------------------------------------------------
# States and lat and longitude tables. Maybe can average them by state? For the weather data as well?
# maybe just take the lat and longitude of the largest county per state? This might make the most sense.

# -----------------------------------------------------------------------
# need to do a join on another table to get population and per capita data

# need to do another join to get for recovered cases and hospitalizations

# -----------------------------------------------------------------------
# Weather Data // can only do 500 requests per day. comment out after loading data, you don't exceed requests....
# need to concatenate all state data frames.
# frequency = 24
# start_date = '1-JAN-2020'
# end_date = '13-MAY-2020'
# api_key = 'c136a1dee5d64cbd93251630201405'
# location_list = ['alabama','alaska', 'arizona','arkansas','california','colorado','connecticut','delaware','district_of_columbia',
#                  'florida','georgia','hawaii', 'idaho', 'illinois' ,'indiana', 'iowa', 'kansas', 'kentucky',
#                  'louisiana', 'maine', 'montana', 'nebraska', 'nevada', 'new_hampshire', 'new_jersey',
#                  'new_mexico','new_york','north_carolina','north_dakota','ohio','oklahoma','oregon','maryland','massachusetts',
#                  'michigan','minnesota','mississippi','missouri','pennsylvania','rhode_island','south_carolina','south_dakota','tennessee',
#                  'texas','utah','vermont','virginia','washington','west_virginia','wisconsin','wyoming']
# hist_weather_data = retrieve_hist_data(api_key,
#                                 location_list,
#                                 start_date,
#                                 end_date,
#                                 frequency,
#                                 location_label = True,
#                                 export_csv = True,
#                                 store_df = True)

# creates 50 different csvs... Then From there we read 50 different csvs into pandas dataframes
# to do data manipulation.
# need to set a variable for date and import the current date and use that variable for the
# historical weather data...
# _______________________________________ alabama_______________________________________
alabama_df = pd.read_csv('alabama.csv')
alabama_df = alabama_df[['date_time','alabama_maxtempC','alabama_mintempC']]
# print(alabama_df.head())
# _______________________________________ alaska_______________________________________
alaska_df = pd.read_csv('alaska.csv')
alaska_df = alaska_df[['alaska_maxtempC','alaska_mintempC']]
# _______________________________________ arizona_______________________________________
arizona_df = pd.read_csv('arizona.csv')
arizona_df = arizona_df[['arizona_maxtempC','arizona_mintempC']]
# _______________________________________ arkansas_______________________________________
arkansas_df = pd.read_csv('arkansas.csv')
arkansas_df = arkansas_df[['arkansas_maxtempC','arkansas_mintempC']]
# _______________________________________ california_______________________________________
california_df = pd.read_csv('california.csv')
california_df = california_df[['california_maxtempC','california_mintempC']]
# _______________________________________ colorado_______________________________________
colorado_df = pd.read_csv('colorado.csv')
colorado_df = colorado_df[['colorado_maxtempC','colorado_mintempC']]
# _______________________________________ connecticut_______________________________________
connecticut_df = pd.read_csv('connecticut.csv')
connecticut_df = connecticut_df[['connecticut_maxtempC','connecticut_mintempC']]
# _______________________________________ delaware_______________________________________
delaware_df = pd.read_csv('delaware.csv')
delaware_df = delaware_df[['delaware_maxtempC','delaware_mintempC']]
# _______________________________________ district_of_columbia_______________________________________
district_of_columbia_df = pd.read_csv('district_of_columbia.csv')
district_of_columbia_df = district_of_columbia_df[['district_of_columbia_maxtempC','district_of_columbia_mintempC']]
# _______________________________________ florida_______________________________________
florida_df = pd.read_csv('florida.csv')
florida_df = florida_df[['florida_maxtempC','florida_mintempC']]
# _______________________________________ georgia_______________________________________
georgia_df = pd.read_csv('georgia.csv')
georgia_df = georgia_df[['georgia_maxtempC','georgia_mintempC']]
# _______________________________________ hawaii_______________________________________
hawaii_df = pd.read_csv('hawaii.csv')
hawaii_df = hawaii_df[['hawaii_maxtempC','hawaii_mintempC']]
# _______________________________________ idaho_______________________________________
idaho_df = pd.read_csv('idaho.csv')
idaho_df = idaho_df[['idaho_maxtempC','idaho_mintempC']]
# _______________________________________ illinois_______________________________________
illinois_df = pd.read_csv('illinois.csv')
illinois_df = illinois_df[['illinois_maxtempC','illinois_mintempC']]
# _______________________________________ indiana_______________________________________
indiana_df = pd.read_csv('indiana.csv')
indiana_df = indiana_df[['indiana_maxtempC','indiana_mintempC']]
# _______________________________________ iowa_______________________________________
iowa_df = pd.read_csv('iowa.csv')
iowa_df = iowa_df[['iowa_maxtempC','iowa_mintempC']]
# _______________________________________ kansas_______________________________________
kansas_df = pd.read_csv('kansas.csv')
kansas_df = kansas_df[['kansas_maxtempC','kansas_mintempC']]
# _______________________________________ kentucky_______________________________________
kentucky_df = pd.read_csv('kentucky.csv')
kentucky_df = kentucky_df[['kentucky_maxtempC','kentucky_mintempC']]
# _______________________________________louisiana_______________________________________
louisiana_df = pd.read_csv('louisiana.csv')
louisiana_df = louisiana_df[['louisiana_maxtempC','louisiana_mintempC']]
# _______________________________________ maine_______________________________________
maine_df = pd.read_csv('maine.csv')
maine_df = maine_df[['maine_maxtempC','maine_mintempC']]
# _______________________________________ montana_______________________________________
montana_df = pd.read_csv('montana.csv')
montana_df = montana_df[['montana_maxtempC','montana_mintempC']]
# _______________________________________ nebraska_______________________________________
nebraska_df = pd.read_csv('nebraska.csv')
nebraska_df = nebraska_df[['nebraska_maxtempC','nebraska_mintempC']]
# _______________________________________ nevada_______________________________________
nevada_df = pd.read_csv('nevada.csv')
nevada_df = nevada_df[['nevada_maxtempC','nevada_mintempC']]
# _______________________________________ new_hampshire_______________________________________
new_hampshire_df = pd.read_csv('new_hampshire.csv')
new_hampshire_df = new_hampshire_df[['new_hampshire_maxtempC','new_hampshire_mintempC']]
# _______________________________________new_jersey_______________________________________
new_jersey_df = pd.read_csv('new_jersey.csv')
new_jersey_df = new_jersey_df[['new_jersey_maxtempC','new_jersey_mintempC']]
# _______________________________________ new_mexico_______________________________________
new_mexico_df = pd.read_csv('new_mexico.csv')
new_mexico_df = new_mexico_df[['new_mexico_maxtempC','new_mexico_mintempC']]
# _______________________________________ new_york_______________________________________
new_york_df = pd.read_csv('new_york.csv')
new_york_df = new_york_df[['new_york_maxtempC','new_york_mintempC']]
# _______________________________________ north_carolina_______________________________________
north_carolina_df = pd.read_csv('north_carolina.csv')
north_carolina_df = north_carolina_df[['north_carolina_maxtempC','north_carolina_mintempC']]
# _______________________________________ north_dakota_______________________________________
north_dakota_df = pd.read_csv('north_dakota.csv')
north_dakota_df = north_dakota_df[['north_dakota_maxtempC','north_dakota_mintempC']]
# _______________________________________ ohio_______________________________________
ohio_df = pd.read_csv('ohio.csv')
ohio_df = ohio_df[['ohio_maxtempC','ohio_mintempC']]
# _______________________________________ oklahoma_______________________________________
oklahoma_df = pd.read_csv('oklahoma.csv')
oklahoma_df = oklahoma_df[['oklahoma_maxtempC','oklahoma_mintempC']]
# _______________________________________ oregon_______________________________________
oregon_df = pd.read_csv('oregon.csv')
oregon_df = oregon_df[['oregon_maxtempC','oregon_mintempC']]
# _______________________________________ maryland_______________________________________
maryland_df = pd.read_csv('maryland.csv')
maryland_df = maryland_df[['maryland_maxtempC','maryland_mintempC']]
# _______________________________________ massachusetts_______________________________________
massachusetts_df = pd.read_csv('massachusetts.csv')
massachusetts_df = massachusetts_df[['massachusetts_maxtempC','massachusetts_mintempC']]
# _______________________________________ michigan_______________________________________
michigan_df = pd.read_csv('michigan.csv')
michigan_df = michigan_df[['michigan_maxtempC','michigan_mintempC']]
# _______________________________________ minnesota_______________________________________
minnesota_df = pd.read_csv('minnesota.csv')
minnesota_df = minnesota_df[['minnesota_maxtempC','minnesota_mintempC']]
# _______________________________________ mississippi_______________________________________
mississippi_df = pd.read_csv('mississippi.csv')
mississippi_df = mississippi_df[['mississippi_maxtempC','mississippi_mintempC']]
# _______________________________________ missouri_______________________________________
missouri_df = pd.read_csv('missouri.csv')
missouri_df = missouri_df[['missouri_maxtempC','missouri_mintempC']]
# _______________________________________ pennsylvania_______________________________________
pennsylvania_df = pd.read_csv('pennsylvania.csv')
pennsylvania_df = pennsylvania_df[['pennsylvania_maxtempC','pennsylvania_mintempC']]
# _______________________________________ rhode_island_______________________________________
rhode_island_df = pd.read_csv('rhode_island.csv')
rhode_island_df = rhode_island_df[['rhode_island_maxtempC','rhode_island_mintempC']]
# _______________________________________ south_carolina_______________________________________
south_carolina_df = pd.read_csv('south_carolina.csv')
south_carolina_df = south_carolina_df[['south_carolina_maxtempC','south_carolina_mintempC']]
# _______________________________________ south_dakota_______________________________________
south_dakota_df = pd.read_csv('south_dakota.csv')
south_dakota_df = south_dakota_df[['south_dakota_maxtempC','south_dakota_mintempC']]
# _______________________________________ tennessee_______________________________________
tennessee_df = pd.read_csv('tennessee.csv')
tennessee_df = tennessee_df[['tennessee_maxtempC','tennessee_mintempC']]
# _______________________________________ texas_______________________________________
texas_df = pd.read_csv('texas.csv')
texas_df = texas_df[['texas_maxtempC','texas_mintempC']]
# _______________________________________ utah_______________________________________
utah_df = pd.read_csv('utah.csv')
utah_df = utah_df[['utah_maxtempC','utah_mintempC']]
# _______________________________________ vermont_______________________________________
vermont_df = pd.read_csv('vermont.csv')
vermont_df = vermont_df[['vermont_maxtempC','vermont_mintempC']]
# _______________________________________ virginia_______________________________________
virginia_df = pd.read_csv('virginia.csv')
virginia_df = virginia_df[['virginia_maxtempC','virginia_mintempC']]
# _______________________________________ washington_______________________________________
washington_df = pd.read_csv('washington.csv')
washington_df = washington_df[['washington_maxtempC','washington_mintempC']]
# _______________________________________ west_virginia_______________________________________
west_virginia_df = pd.read_csv('west_virginia.csv')
west_virginia_df = west_virginia_df[['west_virginia_maxtempC','west_virginia_mintempC']]
# _______________________________________ wisconsin_______________________________________
wisconsin_df = pd.read_csv('wisconsin.csv')
wisconsin_df = wisconsin_df[['wisconsin_maxtempC','wisconsin_mintempC']]
# _______________________________________ wyoming_______________________________________
wyoming_df = pd.read_csv('wyoming.csv')
wyoming_df = wyoming_df[['wyoming_maxtempC','wyoming_mintempC']]
#________________________________________end state Dataframes____________________________
# might make more sense if I just export each one to a csv. Then I can make a seperate dataframe for each one.

# can I make an iterative for loop do commit all of my data frames to seperate sql tables? Do I want to?
# -----------------------------------------------------------------------
# weather data cleaning, drop all columns except for min temp, max temp and humidity
# then concatenate into one table with a lot of columns, VS creating a table for each state in datagrip
# eliminating doing a ton of joins.

agg_weather_df = pd.concat([alabama_df, alaska_df, arizona_df, arkansas_df, california_df, colorado_df, connecticut_df, delaware_df, district_of_columbia_df,
                 florida_df, georgia_df, hawaii_df, idaho_df, illinois_df ,indiana_df, iowa_df, kansas_df, kentucky_df,
                 louisiana_df, maine_df, montana_df, nebraska_df, nevada_df, new_hampshire_df, new_jersey_df,
                 new_mexico_df, new_york_df, north_carolina_df, north_dakota_df, ohio_df, oklahoma_df, oregon_df, maryland_df, massachusetts_df,
                 michigan_df, minnesota_df, mississippi_df, missouri_df, pennsylvania_df, rhode_island_df, south_carolina_df, south_dakota_df, tennessee_df,
                 texas_df, utah_df, vermont_df, virginia_df, washington_df, west_virginia_df, wisconsin_df, wyoming_df], axis = 1)

# print(complete_df.head(50))
agg_weather_df.to_csv('agg_weather_df.csv', index=False)
# this doesn't work right now for some reason all of the columns are empty in the csv after the first state....
# -----------------------------------------------------------------------

# export dataframe after cleaning to csv
states_grouped.to_csv('cov19.csv',index=False)
pop_df.to_csv('pop_cov.csv', index=False)
# -----------------------------------------------------------------------
# DF to sql stuff
# printing columns to build sql tables on
print(states_grouped.columns.tolist())
print(pop_df.columns.tolist())
# -----------------------------------------------------------------------


# -----------------------------------------------------------------------
# Insert whole DataFrame into MySQL, why doesn't this work? # using sqlalchemy doesn't work for somereaseon
pop_sql = pop_df.to_sql('Population_Data', con = engine, if_exists = 'replace', chunksize = 1000)

cursor.execute(pop_sql)

my_db.commit()

states_grouped_sql = states_grouped.to_sql('States_Grouped', con = engine, if_exists = 'replace', chunksize = 1000)
cursor.execute(states_grouped_sql)

my_db.commit()

usa_map_df_sql = usa_map_df.to_sql('maping_data', con = engine, if_exists = 'replace', chunksize = 1000)
cursor.execute(states_grouped_sql)

my_db.commit()



agg_weather_df_sql = agg_weather_df.to_sql('Daily_Weather_Agg', con = engine, if_exists = 'replace', chunksize = 1000)
cursor.execute(agg_weather_df_sql)

my_db.commit()

my_db.close()
#--------------------------------------------------------------------------
# should prob drop table and create a new one every time as well. to keep the most up to date.
# cols = "`,`".join([str(i) for i in df.columns.tolist()])
# # for i, row in df.iterrows():
# #   print('Updating Records..........')
# #   test_sql = "INSERT INTO `updated_data` (`" + cols + "`) VALUES (" + "%s," * (len(row) - 1) + "%s)"
# #   cursor.execute(test_sql, tuple(row))
# #   my_db.commit()








