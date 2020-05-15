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
# _______________________________________ alaska_______________________________________
alaska_df = pd.read_csv('alaska.csv')
# _______________________________________ arizona_______________________________________
arizona_df = pd.read_csv('arizona.csv')
# _______________________________________ arkansas_______________________________________
arkansas_df = pd.read_csv('arkansas.csv')
# _______________________________________ california_______________________________________
california_df = pd.read_csv('california.csv')
# _______________________________________ colorado_______________________________________
colorado_df = pd.read_csv('colorado.csv')
# _______________________________________ connecticut_______________________________________
connecticut_df = pd.read_csv('connecticut.csv')
# _______________________________________ delaware_______________________________________
delaware_df = pd.read_csv('delaware.csv')
# _______________________________________ district_of_columbia_______________________________________
district_of_columbia_df = pd.read_csv('district_of_columbia.csv')
# _______________________________________ florida_______________________________________
florida_df = pd.read_csv('florida.csv')
# _______________________________________ georgia_______________________________________
georgia_df = pd.read_csv('georgia.csv')
# _______________________________________ hawaii_______________________________________
hawaii_df = pd.read_csv('hawaii.csv')
# _______________________________________ idaho_______________________________________
idaho_df = pd.read_csv('idaho.csv')
# _______________________________________ illinois_______________________________________
illinois_df = pd.read_csv('illinois.csv')
# _______________________________________ indiana_______________________________________
indiana_df = pd.read_csv('indiana.csv')
# _______________________________________ iowa_______________________________________
iowa_df = pd.read_csv('iowa.csv')
# _______________________________________ kansas_______________________________________
kansas_df = pd.read_csv('kansas.csv')
# _______________________________________ kentucky_______________________________________
kentucky_df = pd.read_csv('kentucky.csv')
# _______________________________________louisiana_______________________________________
louisiana_df = pd.read_csv('louisiana.csv')
# _______________________________________ maine_______________________________________
maine_df = pd.read_csv('maine.csv')
# _______________________________________ montana_______________________________________
montana_df = pd.read_csv('montana.csv')
# _______________________________________ nebraska_______________________________________
nebraska_df = pd.read_csv('nebraska.csv')
# _______________________________________ nevada_______________________________________
nevada_df = pd.read_csv('nevada.csv')
# _______________________________________ new_hampshire_______________________________________
new_hampshire_df = pd.read_csv('new_hampshire.csv')
# _______________________________________new_jersey_______________________________________
new_jersey_df = pd.read_csv('new_jersey.csv')
# _______________________________________ new_mexico_______________________________________
new_mexico_df = pd.read_csv('new_mexico.csv')
# _______________________________________ new_york_______________________________________
new_york_df = pd.read_csv('new_york.csv')
# _______________________________________ north_carolina_______________________________________
north_carolina_df = pd.read_csv('north_carolina.csv')
# _______________________________________ north_dakota_______________________________________
north_dakota_df = pd.read_csv('north_dakota.csv')
# _______________________________________ ohio_______________________________________
ohio_df = pd.read_csv('ohio.csv')
# _______________________________________ oklahoma_______________________________________
oklahoma_df = pd.read_csv('oklahoma.csv')
# _______________________________________ oregon_______________________________________
oregon_df = pd.read_csv('oregon.csv')
# _______________________________________ maryland_______________________________________
maryland_df = pd.read_csv('maryland.csv')
# _______________________________________ massachusetts_______________________________________
massachusetts_df = pd.read_csv('massachusetts.csv')
# _______________________________________ michigan_______________________________________
michigan_df = pd.read_csv('michigan.csv')
# _______________________________________ minnesota_______________________________________
minnesota_df = pd.read_csv('minnesota.csv')
# _______________________________________ mississippi_______________________________________
mississippi_df = pd.read_csv('mississippi.csv')
# _______________________________________ missouri_______________________________________
missouri_df = pd.read_csv('missouri.csv')
# _______________________________________ pennsylvania_______________________________________
pennsylvania_df = pd.read_csv('pennsylvania.csv')
# _______________________________________ rhode_island_______________________________________
rhode_island_df = pd.read_csv('rhode_island.csv')
# _______________________________________ south_carolina_______________________________________
south_carolina_df = pd.read_csv('south_carolina.csv')
# _______________________________________ south_dakota_______________________________________
south_dakota_df = pd.read_csv('south_dakota.csv')
# _______________________________________ tennessee_______________________________________
tennessee_df = pd.read_csv('tennessee.csv')
# _______________________________________ texas_______________________________________
texas_df = pd.read_csv('texas.csv')
# _______________________________________ utah_______________________________________
utah_df = pd.read_csv('utah.csv')
# _______________________________________ vermont_______________________________________
vermont_df = pd.read_csv('vermont.csv')
# _______________________________________ virginia_______________________________________
virginia_df = pd.read_csv('virginia.csv')
# _______________________________________ washington_______________________________________
washington_df = pd.read_csv('washington.csv')
# _______________________________________ west_virginia_______________________________________
west_virginia_df = pd.read_csv('west_virginia.csv')
# _______________________________________ wisconsin_______________________________________
wisconsin_df = pd.read_csv('wisconsin.csv')
# _______________________________________ wyoming_______________________________________
wyoming_df = pd.read_csv('wyoming.csv')
#________________________________________end state Dataframes____________________________
# might make more sense if I just export each one to a csv. Then I can make a seperate dataframe for each one.
# -----------------------------------------------------------------------
# weather data cleaning, drop all columns except for min temp, max temp and humidity
# then concatenate into one table with a lot of columns, VS creating a table for each state in datagrip
# eliminating doing a ton of joins.

complete_df = pd.concat([alabama_df, alaska_df, arizona_df, arkansas_df, california_df, colorado_df, connecticut_df, delaware_df, district_of_columbia_df,
                 florida_df, georgia_df, hawaii_df, idaho_df, illinois_df ,indiana_df, iowa_df, kansas_df, kentucky_df,
                 louisiana_df, maine_df, montana_df, nebraska_df, nevada_df, new_hampshire_df, new_jersey_df,
                 new_mexico_df, new_york_df, north_carolina_df, north_dakota_df, ohio_df, oklahoma_df, oregon_df, maryland_df, massachusetts_df,
                 michigan_df, minnesota_df, mississippi_df, missouri_df, pennsylvania_df, rhode_island_df, south_carolina_df, south_dakota_df, tennessee_df,
                 texas_df, utah_df, vermont_df, virginia_df, washington_df, west_virginia_df, wisconsin_df, wyoming_df])

# print(complete_df.head(50))
complete_df.to_csv('complete_df.csv', index=False)
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
# # Insert whole DataFrame into MySQL, why doesn't this work? # using sqlalchemy doesn't work for somereaseon
pop_sql = pop_df.to_sql('Population_Data', con = engine, if_exists = 'replace', chunksize = 1000)

cursor.execute(pop_sql)

my_db.commit()

my_db.close()

states_grouped_sql = states_grouped.to_sql('States_Grouped', con = engine, if_exists = 'replace', chunksize = 1000)
cursor.execute(states_grouped_sql)

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








