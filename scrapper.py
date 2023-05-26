import numpy as np
import requests
import re
import json
import pandas as pd
from fake_useragent import UserAgent
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
import time
import pprint
import random
import warnings

warnings.filterwarnings('ignore')

city = 'denver/'

def get_page_urls(number_of_pages: int, city: str) -> list:
    city_url = 'https://www.zillow.com/homes/for_sale/' + city
    city_urls = [city_url]
    for i in range(1, number_of_pages):
        tmp_url = 'https://www.zillow.com/homes/for_sale/' + city + f'/{i}_p/'
        city_urls.append(tmp_url)
    return city_urls

def get_details_page_url(dataframe):
    zillow_ids = list(dataframe['zpid'])
    zillow_details_url = []
    for id in zillow_ids:
        tmp = f'https://www.zillow.com/homes/for_sale/{id}_zpid'
        zillow_details_url.append(tmp)
    return zillow_details_url

def data_list(urls):
    ua = UserAgent()
    req_headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
        'accept-encoding': 'gzip, deflate, br',
        'accept-language': 'en-US,en;q=0.8',
        'upgrade-insecure-requests': '1',
        'User-Agent': f'{str(ua.random)}'
    }
    housing_data = []
    for url in urls:
        with requests.session() as session:
            tmp_request = session.get(url, headers=req_headers)

            tmp_data = json.loads(re.search(r'!--(\{"queryState".*?)-->', tmp_request.text).group(1))
            housing_data.append(tmp_data)
    return housing_data

def get_house_details(urls):
    house_data = {}
    completed = 0
    for url in urls:
        historical_data = {'Neighborhood': "", '2010': {}, '2011': {}, '2012': {},
                           '2013': {}, '2014': {}, '2015': {}, '2016': {}, '2017': {},
                           '2018': {}, '2019': {}, '2020': {}, '2021': {}, '2022': {}}

        safari_options = Options()
        safari_options.headless = True
        driver = webdriver.Firefox(options=safari_options)
        driver.get(url)
        time.sleep(random.randint(3, 9))  # ensure the page is fully loaded

        button = driver.find_element(By.XPATH, '//*[@id="ds-home-values"]/div/div[1]/div/div/div[3]/div/button/p')
        button.click()
        time.sleep(random.randint(1, 4))
        # Render the dynamic content to static HTML
        html = driver.page_source
        n = html.find('neighborhoodRegion')
        neigh = html[n:]
        neigh = list(neigh.split("\\"))
        neigh = neigh[4][1:]
        historical_data['Neighborhood'] = neigh
        # Parse the static HTML
        soup = BeautifulSoup(html, "html.parser")
        divs = str(soup.find("tbody", {"class": "StyledTableBody-c11n-8-73-0__sc-8i1s74-0 jSTHjy"}))
        di = list(divs.split("<td"))
        stuff = []
        for i in range(1, len(di)):
            tmp = di[i][58:]
            tmp = list(tmp.split('</td>'))
            stuff.append(tmp[0])

        for i in range(0, len(stuff), 4):
            x = i
            tmp = stuff[x:x + 2]
            tmp_dates = tmp[0].split(" ")
            tmp_dates.reverse()
            tmp_dates.append(tmp[1])
            if len(tmp_dates) == 3:
                year = tmp_dates[0]
                month = tmp_dates[1]
                value = tmp_dates[2]
                if(int(year) >= 2010):
                    historical_data[year][month] = value
        # Close the webdriver
        driver.close()

        house_data[url] = historical_data
        with open('url_done.txt', 'a') as f:
            f.write(url+'\n')
        completed += 1
        print("Completed:", completed,"   " ,url)

    return house_data

def get_data_frame(data: list):
    tmp_df = pd.DataFrame()
    for i in data:
        for item in i['cat1']['searchResults']['listResults']:
            tmp_df = tmp_df.append(item, ignore_index=True)
    return tmp_df

def get_list_of_columns(dataframe):
    columns = []
    for col in dataframe.columns:
        columns.append(col)
    return columns


def return_merged_dataFrame(data, data_frame):
    urls = data.keys()
    for url in urls:
        year_details = data[url]
        years = year_details.keys()
        for year in years:
            if year == 'Neighborhood':
                continue
            row_data = data_frame[data_frame['detailUrl'] == url].iloc[0]
            row_data['Year'] = year
            row_data['Neighborhood'] = data[url]['Neighborhood']
            months_data = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
            for month in months_data:
                try:
                    row_data[month] = data[url][year][month]
                except KeyError:
                    continue

            data_frame = data_frame.append(row_data)

    return data_frame


urls = get_page_urls(1, 'denver')
data = data_list(urls)
df = get_data_frame(data)
columns_to_drop = ['providerListingId', 'imgSrc', 'hasImage', 'statusType', 'statusText', 'countryCurrency',
                   'isUndisclosedAddress', 'beds', 'baths', 'area', 'latLong', 'isZillowOwned',
                   'variableData', 'badgeInfo', 'hdpData', 'isSaved', 'isUserClaimingOwner',
                   'isUserConfirmedClaim', 'pgapt', 'sgapt', 'shouldShowZestimateAsPrice',
                   'has3DModel', 'hasVideo', 'isHomeRec', 'info3String', 'info1String', 'brokerName',
                   'hasAdditionalAttributions', 'isFeaturedListing', 'availabilityDate', 'list', 'relaxed',
                   'hasOpenHouse', 'openHouseDescription', 'openHouseEndDate', 'openHouseStartDate',
                   'unformattedPrice', 'id']

df = df.drop(columns=columns_to_drop)
months = ['Year', 'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
df[months] = ''
df['Neighborhood'] = ''
df.to_csv('house_og.csv')
house_details_url = list(df['detailUrl'])
house_urls = set(house_details_url)
house_details_url = list(house_urls)

chunked_list = list()
chunk_size = 10
for i in range(0, len(house_details_url), chunk_size):
    chunked_list.append(house_details_url[i:i+chunk_size])

for i in chunked_list:
    data = get_house_details(i)
    df = return_merged_dataFrame(data, df)
    df.to_csv('data.csv')
    print("Check file")
    time.sleep(random.randint(5, 20))

