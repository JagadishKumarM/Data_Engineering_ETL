# %%
import requests
from bs4 import BeautifulSoup as bs
import pandas as pd
import unicodedata
import sqlalchemy as sa
from sqlalchemy import inspect, text

#importing the necessary python librabries required for webscraping and make connection to Mysql Database and use other SQL functionalities. 

# %%
# Webscraping from wikipedia

cities = ['Bangalore','Chennai','Hyderabad','Delhi','Kanpur','Patna']
#The above line defines a list called cities, which contains the names of cities to be scraped from Wikipedia.

#Defining a method called city_info and passing a parameter soup.
def City_info(soup):
    ret_dict = {} #creating an empty dictionary ret_dict
    ret_dict['City_Name'] = soup.h1.get_text()
    #soup- The parameter is coming from the main function containing the parsed HTML web contents stored using the beautifulsoup library
    #<h1> represents header in the HTML structure.      
    

#Scraping the required elements form the webpage from the parsed HTML page of Wikipedia. 
    
    area_element_MC = soup.select_one('.infobox-label:-soup-contains("Megacity") + .infobox-data')
    area_element_C = soup.select_one('.infobox-label:-soup-contains("City") + .infobox-data')
    elevation_element = soup.select_one('.infobox-label:-soup-contains("Elevation") + .infobox-data')
    metro= soup.select_one('.infobox-label:-soup-contains("Metro") + .infobox-data')
    population = soup.select_one('.mergedtoprow:-soup-contains("Population")')
    latitude = soup.select_one('.latitude')
    longitude = soup.select_one('.longitude')
    
    
    ''' Checking if the elements required to be scraped are found, if found proceeding to extract the same using .get_text(strip=True).
        The extracted text is normalized using unicodedata.normalize() and stored in their respective key's of ret_dict. '''
         
    if area_element_MC is not None:
        ret_dict['City_size_Km2'] = unicodedata.normalize('NFC', area_element_MC.get_text(strip=True))
    
    if area_element_C is not None:
        ret_dict['City_size_Km2'] = unicodedata.normalize('NFC', area_element_C.get_text(strip=True))
        
    if elevation_element is not None: 
        ret_dict['Elevation_in_M'] = unicodedata.normalize('NFC', elevation_element.get_text(strip=True)) 
        
    if population is not None:
        ret_dict['Population'] = unicodedata.normalize('NFC', population.findNext('td').get_text(strip=True))
        
    if metro is not None:
        ret_dict['Metro_Km2'] = unicodedata.normalize('NFC', metro.get_text(strip=True))
         
    if latitude is not None:
        ret_dict['Latitude']= unicodedata.normalize('NFC', latitude.get_text(strip=True))
        
    if longitude is not None:
        ret_dict['Longitude']= unicodedata.normalize('NFC', longitude.get_text(strip=True))

    return ret_dict

list_of_city_info = []
for city in cities:
    url = 'https://en.wikipedia.org/wiki/{}'.format(city)
    web = requests.get(url)
    soup = bs(web.content, 'html.parser')
    list_of_city_info.append(City_info(soup))

'''The above for loop iterates over the defined cities list via the loop element city.
The requests library is used to constructs the URL for the Wikipedia page of the city.
and fetch the page content thorugh get() function.
Once the page contents is fetched we make use of the Beautfiulsoup library to parse the HTML content to the defined variable soup. 
On calling the defined function "City_info" which extracts all the information and appends the returned dictionary to list "list_of_city_info".'''

df_cities = pd.DataFrame(list_of_city_info,index=range(1, len(cities) + 1))
print(df_cities)

# Finally we make use of the Pandas to create a DataFrame called "df_cities" from the list of dictionaries list_of_city_info. 
# %%
# Removing commas and extracting only numeric values from multiple columns
columns_to_process = ['City_size_Km2', 'Elevation_in_M', 'Population','Metro_Km2']

for column in columns_to_process:
    df_cities[column] = df_cities[column].replace(',', '', regex=True).str.extract('(\d+)', expand=False)

# Convert the columns to numeric data type
df_cities[columns_to_process] = df_cities[columns_to_process].apply(pd.to_numeric)


# %%
df_cities

# %%

# Uploading the scraped data to a database, modify the details as per your local MySql/AWS RDS MySql database. 
schema="report"
host="127.0.0.1"
user="root"
password="Rathna03$"
port=3306
con = f'mysql+pymysql://{user}:{password}@{host}:{port}/{schema}'

'''The above code constructs a connection to a MySQL database using SQLAlchemy and the PyMySQL driver, 
   with the specified database schema, hostname, username, password, and port number.'''

# Create a SQLAlchemy engine to work with a database.
#It provides an interface for executing SQL statements, managing transactions, and interacting with the database in a Pythonic way.
engine = sa.create_engine(con)

# Create an inspector object
inspector = inspect(engine)
'''The inspector object allows you to gather metadata about the database schema,structure of the database,table names, 
column details, indexes, and constraints.'''

df_cities.dropna().to_sql('cities', con=engine, if_exists='replace', index=False)

'''The above line writes the contents of the DataFrame df_cities to a table named 'cities' in the database connected through the SQLAlchemy engine.
dropna() is used to drop any rows with missing values from the DataFrame before writing it to the table'''

connection = engine.connect() # Creation and establishment of a connection to the database via creation of execution engine.
Update_stmt=text("UPDATE cities SET Metro_Km2=52500 WHERE City_Name='Delhi'") 
#The text() function provided by SqlAlchemhy is used to parse the raw SQL data in python and excetue the command with the help of execution engine.
connection.execute(Update_stmt) 
connection.commit()#Commit to the executed operation to make the changes permanent
connection.close()


# %%
df_cities.loc[df_cities['City_Name'] == 'Delhi', 'Metro_Km2']=52500

# %%
df_cities

# %%
'''Scarping data from OpenWeatherMap website (https://openweathermap.org/), 
these commands help fetch the pyowm packages and its modules,which allows you to access weather data and forecast via OpenWeatherMap '''

from pyowm import OWM 
from pyowm.utils import config
from pyowm.utils import timestamps 

# Defining a list of cities for which weather forecast data will be fetched from the country "IND"
cities = ['Bangalore','Chennai','Hyderabad','Delhi','Kanpur','Patna']
country = "IND"
#This variable stores the API key required to authenticate requests to the OpenWeatherMap API.
OWM_key = "a42aed5b04bd047955b4463beb08e476" 

'''The below loop iterates over each city in the cities list.
   For each city, it makes a GET request to the OpenWeatherMap API to fetch the weather forecast data.
   It then extracts relevant information from the JSON response and constructs a 'weather_hour' dictionary which is appended to the weather_info list.'''
   
weather_info = [] 
for city in cities:
   response = requests.get(f'http://api.openweathermap.org/data/2.5/forecast/?q={city},{country}&appid={OWM_key}&units=metric&lang=en')
   forecast_api = response.json()['list']
   
   '''The above line of code retrieves the list of weather forecasts for the specified city returned by the OpenWeatherMap API & 
      assigns it to the variable forecast_api for further processing.
      The ' .json() ' is used to parse JSON data (a string) into a Python dictionary, making it easier to work with JSON data in Python.
      The OpenWeatherMap API response JSON typically contains several keys, including 'list', 
      which holds a list of weather forecasts for the specified location. The value associated with the key 'list' is assigned to the variable forecast_api.'''
      
   for forecast_3h in forecast_api: 
         weather_hour = {}
         # Retriving the Datetime - 'dt_txt' is the key in which the datetime value is stored within the parsed JSON to python dictionary-->'forecast_api'
         weather_hour['Datetime'] = forecast_3h['dt_txt']
         # temperature 
         weather_hour['Temperature'] = forecast_3h['main']['temp']
         # wind
         weather_hour['Wind_m/s'] = forecast_3h['wind']['speed']
         # rain quantity
         try: weather_hour['rain_qty_mm'] = float(forecast_3h['rain']['3h'])
         except: weather_hour['rain_qty_mm'] = 0
         # probability precipitation 
         try: weather_hour['Prob_perc_mm'] = float(forecast_3h['pop'])
         except: weather_hour['prob_perc_mm'] = 0
         # Weather description
         weather_hour['Weather_decpription'] = forecast_3h['weather'][0]['description']
         #In this specific case, the zero is used because there is only one weather condition in the list, and it's being accessed by its index, which is 0.
         weather_hour['Humidity_g/m3'] = forecast_3h['main']['humidity']
         weather_hour['Pressure_N/m2'] = forecast_3h['main']['pressure']
         weather_hour['City_Name'] = city
   
         weather_info.append(weather_hour)
weather_data = pd.DataFrame(weather_info)
# Finally we make use of the Pandas to create a DataFrame called "weather_data".

# %%
# Making use of the pandas functions to analyze the data scraped into a data frame.
weather_data.head(239)
weather_data.describe()
weather_data.info()


weather_data["Datetime"] = pd.to_datetime(weather_data["Datetime"])
#It is found that the 'Datetime' column scraped is of STRING type and the above line converts it into datetime format.

# Extracting Date, Time, Day_of_Week from 'datetime' using pandas feature that provides access to various datetime properties and methods.
weather_data['Date'] = weather_data['Datetime'].dt.date
weather_data['Time'] = weather_data['Datetime'].dt.time
weather_data['Day_of_Week'] = weather_data['Datetime'].dt.day_name()

weather_data.drop(columns=['Datetime'],inplace=True)

weather_data

# %%
weather_data.dropna().to_sql('forecast',con=engine,if_exists='append',index=False)

''' Finally we write the contents of the 'weather_data' dataFrame to a table named 'forecast' in the desired database connected through the SQLAlchemy engine.
if_exists='append' adds the forecast data to the forecast table present in the database  which is scheduled to run once in every 5 days thorugh Lambda function defined in AWS Console.'''

