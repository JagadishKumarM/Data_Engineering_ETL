%pip install pyowm 
from pyowm import OWM 
from pyowm.utils import config
from pyowm.utils import timestamps
import requests
from bs4 import BeautifulSoup as bs
import pandas as pd

cities = ['Bangalore','Chennai','Hyderabad','Delhi','Kanpur','Patna']
country = "IND"
OWM_key = "your_openweathermap_api_key"
weather_info = []

for city in cities:
    response = requests.get(f'http://api.openweathermap.org/data/2.5/forecast/?q={city},IND&appid={OWM_key}&units=metric')
    forecast_api = response.json()['list']
    
    for forecast_3h in forecast_api:
        weather_hour = {}
        weather_hour['Datetime'] = forecast_3h['dt_txt']
        weather_hour['Temperature'] = forecast_3h['main']['temp']
        weather_hour['Wind_m/s'] = forecast_3h['wind']['speed']
        weather_hour['Humidity_g/m3'] = forecast_3h['main']['humidity']
        weather_hour['Pressure_N/m2'] = forecast_3h['main']['pressure']
        weather_hour['City_Name'] = city
        
        weather_info.append(weather_hour)

weather_data = pd.DataFrame(weather_info)
print(weather_data)
