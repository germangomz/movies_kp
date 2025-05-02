import requests
import json
import pandas as pd
import os

pd.options.display.float_format = '{:,.1f}'.format

TOKEN = 'TS5EEXJ-8CN4Y49-NK7X038-09NB6G8'
URL = 'https://api.kinopoisk.dev/v1.4/movie'
FILE_PATH = '/Users/mac/datasets/kp/df3.json'

params = {
    'type': 'movie',
    'selectFields': ['id', 'name', 'year', 'rating', 'ageRating', 'votes', 'seasonsInfo', 'budget', 'audience', 
                     'seriesLength', 'totalSeriesLength', 'genres', 'countries', 'networks', 'fees', 
                     'sequelsAndPrequels', 'updatedAt', 'createdAt'],        
    'page': 1,                        
    'limit': 250,                   
    'sortField': 'rating.kp',
    'sortType': [-1],                 
    'token': TOKEN                   
}

def load_existing_data():
    if os.path.exists(FILE_PATH) and os.path.getsize(FILE_PATH) > 0:
        with open(FILE_PATH, 'r', encoding='utf-8') as f:
            try:
                data = json.load(f)
                return data if isinstance(data, list) else []
            except json.JSONDecodeError:
                return []
    return []

def fetch_movies(existing_data):
    series = existing_data.copy()
    last_page = (len(series) // params['limit']) + 1 
    page = last_page

    while len(series) < 1_000_000:
        params['page'] = page
        response = requests.get(URL, params=params, headers={'accept': 'application/json', 'X-API-KEY': TOKEN})

        if response.status_code != 200:
            print(f'Ошибка. {response.status_code}, {response.text}')
            break

        data = response.json()

        if 'docs' not in data or not data['docs']:
            break

        for ser in data['docs']:
            series_data = {
                'title': ser.get('name', ''),
                'year': ser.get('year', ''),
                'rating_kp': ser.get('rating', {}).get('kp', ''),
                'rating_imdb': ser.get('rating', {}).get('imdb', ''),
                'ageRating': ser.get('ageRating', ''),
                'votes_kp': ser.get('votes', {}).get('kp', ''),
                'votes_imdb': ser.get('votes', {}).get('imdb', ''),
                'seasonsInfo': ser.get('seasonsInfo', ''),
                'budget': ser.get('budget', ''),
                'audience': ser.get('audience', ''),
                'episodeLength': ser.get('seriesLength', ''),
                'totalSeriesLength': ser.get('totalSeriesLength', ''),
                'genre': ser.get('genres', ''),
                'country': ', '.join(country.get('name', '') for country in ser.get('countries', [])),
                'platform': ser.get('networks', ''),
                'fees': ser.get('fees', ''),
                'fees_world': ser.get('fees', {}).get('world', ''),
                'fees_usa': ser.get('fees', {}).get('usa', ''),
                'fees_russia': ser.get('fees', {}).get('russia', ''),
                'updatedAt': ser.get('updatedAt', ''),
                'createdAt': ser.get('createdAt', '')
            }
            series.append(series_data)

        page += 1
        print(f'Загружена страница {page}, всего фильмов: {len(series)}')

    return series

existing_data = load_existing_data()

updated_data = fetch_movies(existing_data)

with open(FILE_PATH, 'w', encoding='utf-8') as f:
    json.dump(updated_data, f, ensure_ascii=False, indent=4)

df3 = pd.read_json(FILE_PATH)
