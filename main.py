import json
import pandas as pd
import random
import requests

from hashlib import sha1
from os import system
from time import time
from sqlalchemy import create_engine



def get_regions(url, headers):
    """
    >>> get_regions(url = "https://parseapi.back4app.com/classes//Continent",headers = { 'X-Parse-Application-Id': 'mxsebv4KoWIGkRntXwyzg6c6DhKWQuit8Ry9sHja','X-Parse-Master-Key': 'TpO0j3lG2PmEVMXlKYQACoOXKQrL3lwM0HwR9dbH' })
    ['Africa', 'Oceania', 'Europe', 'Asia', 'America']
    >>> get_regions(url = "https://parseapi.back4app.com/classes//Continentr",headers = { 'X-Parse-Application-Id': 'mxsebv4KoWIGkRntXwyzg6c6DhKWQuit8Ry9sHja','X-Parse-Master-Key': 'TpO0j3lG2PmEVMXlKYQACoOXKQrL3lwM0HwR9dbH' })
    Traceback (most recent call last):
    ...
    AssertionError: Error page not found
    """
    data = json.loads(requests.get(url, headers=headers).content.decode('utf-8'))
    assert data.get("results"), "Error page not found"
    regions= [region['name'] for region in data['results']]
    regions+= ['America']
    regions.remove('North America')
    regions.remove('South America')
    regions.remove('Antarctica')
    return regions
 

def results(data, inicial_time):
    
        print("Total records creation time only into dataframe, it don't include query time: \n" 
            + str(data['Time'].sum()*1000)+" ms" )
        print("Average time to create a record in dataframe: " + str(data['Time'].mean()*1000)+" ms")
        print("Maximun time: " + str(data['Time'].max()*1000)+" ms")
        print("Minimun time: " + str(data['Time'].min()*1000)+" ms")
        final = time()
        print("total execution time: " + str(final-inicial_time) +" s")

        return None
 

def save_records(data):

    #export to sqlite
    engine = create_engine('sqlite:///./data.sqlite', echo = False)
    data.to_sql('records', con = engine)
    #export json
    data.to_json('./data.json',orient='records')

def run():

    """
    >>> df,b = run(); df.isnull().values.any(); len(df); b>0.000
    False
    245
    True
    >>> record = random.choice(df['Languages']); record.isalpha()
    False
    """

    #initial variables
    initial_time2 = time()
    url_regions = "https://parseapi.back4app.com/classes//Continent"
    url_countries = 'https://restcountries.com/v3.1/region/'
    headers = {
        'X-Parse-Application-Id': 'mxsebv4KoWIGkRntXwyzg6c6DhKWQuit8Ry9sHja',
        'X-Parse-Master-Key': 'TpO0j3lG2PmEVMXlKYQACoOXKQrL3lwM0HwR9dbH' 
    }
    df1 = pd.DataFrame(columns = ['Region', 'City Name', 'Languages', 'Time'])

    regions = get_regions(url_regions, headers)
    for region in regions:
        data_countries = json.loads(requests.get(url_countries + region).content.decode('utf-8'))
        for country in data_countries:
            initial_time = time()
            languages = list(country['languages'].values())
            languages_encrpt = sha1(','.join(languages).encode('utf-8')).hexdigest()
            final_time = time()
            execution = final_time - initial_time
            df1 = df1.append({
                'Region': region, 'City Name':country['name']['common'], 
                'Languages':languages_encrpt, 'Time': execution
                }, ignore_index=True)

    df1['Time'].astype('float64')
    system("clear")
    
    return df1, initial_time2


if __name__=='__main__':
    
    print("Welcome")
    df,initial_time = run()
    results(df,initial_time)
    save_records(df)


    input("\n \npress ENTER to continue with test")
    import doctest
    doctest.testmod(verbose=True)