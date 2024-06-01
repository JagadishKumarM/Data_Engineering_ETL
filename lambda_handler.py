import requests
from bs4 import BeautifulSoup as bs
import pandas as pd
import unicodedata
import sqlalchemy as sa
from sqlalchemy import inspect, text
from pyowm import OWM
from pyowm.utils import config
from pyowm.utils import timestamps

schema = "report"
host = "127.0.0.1"
user = "root"
port = 3306
con = f'mysql+pymysql://{user}:{password}@{host}:{port}/{schema}'

engine = sa.create_engine(con)

inspector = inspect(engine)

cities = ['Bangalore', 'Chennai', 'Hyderabad', 'Delhi', 'Kanpur', 'Patna']

def lambda_handler(event, context):

    def City_info(soup):
        ret_dict = {}
        ret_dict['City_Name'] = soup.h1.get_text()
        
        area_element_MC = soup.select_one('.infobox-label:-soup-contains("Megacity") + .infobox-data')
        area_element_C = soup.select_one('.infobox-label:-soup-contains("City") + .infobox-data')
        elevation_element = soup.select_one('.infobox-label:-soup-contains("Elevation") + .infobox-data')
        metro = soup.select_one('.infobox-label:-soup-contains("Metro") + .infobox-data')
        population = soup.select_one('.mergedtoprow:-soup-contains("Population")')
        latitude = soup.select_one('.latitude')
        longitude = soup.select_one('.longitude')
        
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
            ret_dict['Latitude'] = unicodedata.normalize('NFC', latitude.get_text(strip=True))
            
        if longitude is not None:
            ret_dict['Longitude'] = unicodedata.normalize('NFC', longitude.get_text(strip=True))

        return ret_dict

    list_of_city_info = []
    for city in cities:
        url = 'https://en.wikipedia.org/wiki/{}'.format(city)
        web = requests.get(url)
        soup = bs(web.content, 'html.parser')
        list_of_city_info.append(City_info(soup))

    df_cities = pd.DataFrame(list_of_city_info, index=range(1, len(cities) + 1))

    columns_to_process = ['City_size_Km2', 'Elevation_in_M', 'Population', 'Metro_Km2']

    for column in columns_to_process:
        df_cities[column] = df_cities[column].replace(',', '', regex=True).str.extract('(\d+)', expand=False)

    df_cities[columns_to_process] = df_cities[columns_to_process].apply(pd.to_numeric)

    df_cities.dropna().to_sql('cities', con=engine, if_exists='replace', index=False)

    connection = engine.connect()
    Update_stmt = text("UPDATE cities SET Metro_Km2=52500 WHERE City_Name='Delhi'")
    connection.execute(Update_stmt)
    connection.commit()
    connection.close()

    df_cities.loc[df_cities['City_Name'] == 'Delhi', 'Metro_Km2'] = 52500
    
    cities = ['Bangalore', 'Chennai', 'Hyderabad', 'Delhi', 'Kanpur', 'Patna']
    country = "IND"
    OWM_key = "a42aed5b04bd047955b4463beb08e476"

    weather_info = []
    for city in cities:
        response = requests.get(f'http://api.openweathermap.org/data/2.5/forecast/?q={city},{country}&appid={OWM_key}&units=metric&lang=en')
        forecast_api = response.json()['list']
        
        for forecast_3h in forecast_api: 
            weather_hour = {}
            weather_hour['Datetime'] = forecast_3h['dt_txt']
            weather_hour['Temperature'] = forecast_3h['main']['temp']
            weather_hour['Wind_m/s'] = forecast_3h['wind']['speed']
            try: weather_hour['rain_qty_mm'] = float(forecast_3h['rain']['3h'])
            except: weather_hour['rain_qty_mm'] = 0
            try: weather_hour['Prob_perc_mm'] = float(forecast_3h['pop'])
            except: weather_hour['prob_perc_mm'] = 0
            weather_hour['Weather_decpription'] = forecast_3h['weather'][0]['description']
            weather_hour['Humidity_g/m3'] = forecast_3h['main']['humidity']
            weather_hour['Pressure_N/m2'] = forecast_3h['main']['pressure']
            weather_hour['City_Name'] = city
            weather_info.append(weather_hour)

    weather_data = pd.DataFrame(weather_info)

    weather_data["Datetime"] = pd.to_datetime(weather_data["Datetime"])

    weather_data['Date'] = weather_data['Datetime'].dt.date
    weather_data['Time'] = weather_data['Datetime'].dt.time
    weather_data['Day_of_Week'] = weather_data['Datetime'].dt.day_name()

    weather_data.drop(columns=['Datetime'], inplace=True)

    weather_data.dropna().to_sql('forecast', con=engine, if_exists='append', index=False)
