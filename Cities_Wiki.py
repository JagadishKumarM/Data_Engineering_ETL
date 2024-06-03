import requests
from bs4 import BeautifulSoup as bs
import pandas as pd
import unicodedata

cities = ['Bangalore', 'Chennai', 'Hyderabad', 'Delhi', 'Kanpur', 'Patna']

def City_info(soup):
    ret_dict = {}
    ret_dict['City_Name'] = soup.h1.get_text()
    
    # Scraping specific elements
    area_element_MC = soup.select_one('.infobox-label:-soup-contains("Megacity") + .infobox-data')
    area_element_C = soup.select_one('.infobox-label:-soup-contains("City") + .infobox-data')
    elevation_element = soup.select_one('.infobox-label:-soup-contains("Elevation") + .infobox-data')
    metro = soup.select_one('.infobox-label:-soup-contains("Metro") + .infobox-data')
    population = soup.select_one('.mergedtoprow:-soup-contains("Population")')
    latitude = soup.select_one('.latitude')
    longitude = soup.select_one('.longitude')
    
    if area_element_MC:
        ret_dict['City_size_Km2'] = unicodedata.normalize('NFC', area_element_MC.get_text(strip=True))
    if area_element_C:
        ret_dict['City_size_Km2'] = unicodedata.normalize('NFC', area_element_C.get_text(strip=True))
    if elevation_element: 
        ret_dict['Elevation_in_M'] = unicodedata.normalize('NFC', elevation_element.get_text(strip=True)) 
    if population:
        ret_dict['Population'] = unicodedata.normalize('NFC', population.findNext('td').get_text(strip=True))
    if metro:
        ret_dict['Metro_Km2'] = unicodedata.normalize('NFC', metro.get_text(strip=True))
    if latitude:
        ret_dict['Latitude'] = unicodedata.normalize('NFC', latitude.get_text(strip=True))
    if longitude:
        ret_dict['Longitude'] = unicodedata.normalize('NFC', longitude.get_text(strip=True))

    return ret_dict

list_of_city_info = []
for city in cities:
    url = f'https://en.wikipedia.org/wiki/{city}'
    web = requests.get(url)
    soup = bs(web.content, 'html.parser')
    list_of_city_info.append(City_info(soup))

df_cities = pd.DataFrame(list_of_city_info)
print(df_cities)
