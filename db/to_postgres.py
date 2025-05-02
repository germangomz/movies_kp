import psycopg2
import json
from psycopg2.extras import Json

conn = psycopg2.connect(
    dbname="postgres",
    user="",  
    password="",
    host="localhost",
    port="5432"
)
cursor = conn.cursor()

with open('/Users/mac/datasets/kp/df3.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

for movie in data:
    title = movie.get('title')
    if not title:
        print(f"Пропущен фильм без названия: {movie}")
        continue

    genres = [genre['name'] for genre in movie.get('genre', [])] if isinstance(movie.get('genre'), list) else None

    updated_at = movie.get('updatedAt') or None
    created_at = movie.get('createdAt') or None

    if updated_at == "": updated_at = None
    if created_at == "": created_at = None

    fees_world = Json(movie.get('fees_world') if isinstance(movie.get('fees_world'), dict) else {})
    fees_usa = Json(movie.get('fees_usa') if isinstance(movie.get('fees_usa'), dict) else {})
    fees_russia = Json(movie.get('fees_russia') if isinstance(movie.get('fees_russia'), dict) else {})

    cursor.execute("""
        INSERT INTO movies (
            title, year, rating_kp, rating_imdb, age_rating, votes_kp, votes_imdb,
            episode_length, total_series_length, country, platform, genres,
            updated_at, created_at, fees_world, fees_usa, fees_russia
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s
        )
    """, (
        title,
        movie.get('year'),
        movie.get('rating_kp'),
        movie.get('rating_imdb'),
        movie.get('ageRating'),
        movie.get('votes_kp'),
        movie.get('votes_imdb'),
        movie.get('episodeLength'),
        movie.get('totalSeriesLength'),
        movie.get('country') if isinstance(movie.get('country'), str) else None,
        movie.get('platform') if isinstance(movie.get('platform'), str) else None,
        genres,
        updated_at,
        created_at,
        fees_world,
        fees_usa,
        fees_russia
    ))

conn.commit()
cursor.close()
conn.close()