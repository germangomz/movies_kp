CREATE TABLE movies (
    id SERIAL PRIMARY KEY,
    title TEXT NOT NULL,
    year INTEGER,
    rating_kp NUMERIC(4, 3),
    rating_imdb NUMERIC(4, 3),
    age_rating INTEGER,
    votes_kp INTEGER,
    votes_imdb INTEGER,
    episode_length INTEGER,
    total_series_length INTEGER,
    country TEXT,
    platform TEXT,
    genres TEXT[],
    updated_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE,
    fees_world JSONB,
    fees_usa JSONB,
    fees_russia JSONB
);

SELECT *
from movies

ALTER TABLE movies
DROP COLUMN episode_length,
DROP COLUMN total_series_length,
DROP COLUMN platform;

CREATE TABLE movies_backup AS SELECT * FROM movies;

SELECT *
from movies_backup

ALTER TABLE movies ADD COLUMN IF NOT EXISTS fees_world_value NUMERIC;

UPDATE movies
SET fees_world_value = (fees_world->>'value')::NUMERIC
WHERE fees_world IS NOT NULL

ALTER TABLE movies ADD COLUMN IF NOT EXISTS fees_usa_value NUMERIC;

UPDATE movies
SET fees_usa_value = (fees_usa->>'value')::NUMERIC
WHERE fees_usa IS NOT null

ALTER TABLE movies ADD COLUMN IF NOT EXISTS fees_russia_value NUMERIC;

UPDATE movies
SET fees_russia_value = (fees_russia->>'value')::NUMERIC
WHERE fees_russia IS NOT NULL

ALTER TABLE movies
DROP COLUMN fees_world,
DROP COLUMN fees_usa,
DROP COLUMN fees_russia;

UPDATE movies
SET age_rating = 0
WHERE age_rating IS NULL;

UPDATE movies
SET genres = ARRAY['другое']::TEXT[]
WHERE genres IS NULL OR array_length(genres, 1) IS NULL;

ALTER TABLE movies ADD COLUMN IF NOT EXISTS fees_sum NUMERIC;

UPDATE movies
SET fees_sum = COALESCE(fees_world_value, 0) 
             + COALESCE(fees_usa_value, 0) 
             + COALESCE(fees_russia_value, 0)
WHERE fees_world_value IS NOT NULL 
   OR fees_usa_value IS NOT NULL 
   OR fees_russia_value IS NOT NULL;
--Views
--1
CREATE VIEW top_rated_movies_kp AS
SELECT title, year, rating_kp, country
FROM movies
WHERE rating_kp > (SELECT AVG(rating_kp) FROM movies WHERE rating_kp > 0)
ORDER BY rating_kp DESC;

SELECT *
FROM top_rated_movies_kp

--2
CREATE VIEW top_rated_movies_imdb AS
SELECT title, year, rating_imdb, country
FROM movies
WHERE rating_imdb > (SELECT AVG(rating_imdb) FROM movies WHERE rating_imdb > 0)
ORDER BY rating_imdb DESC;

SELECT *
FROM top_rated_movies_imdb

--3
CREATE VIEW avg_rating_by_country AS
SELECT 
    country,
    COUNT(*) AS movies_count,
    ROUND(AVG(rating_kp), 2) AS avg_rating_kp,
    ROUND(AVG(rating_imdb), 2) AS avg_rating_imdb
FROM movies
WHERE country IS NOT NULL AND rating_kp > 0 AND rating_imdb > 0
GROUP BY country
HAVING COUNT(*) > 5
ORDER BY movies_count DESC;

SELECT *
FROM avg_rating_by_country

--4
CREATE VIEW movies_by_year AS
SELECT 
    year,
    COUNT(*) AS movies_count,
    ROUND(AVG(rating_kp), 2) AS avg_rating_kp,
    ROUND(AVG(rating_imdb), 2) AS avg_rating_imdb
FROM movies
GROUP BY year
ORDER BY year;

SELECT *
FROM movies_by_year

--5
CREATE VIEW rating_votes_kp AS
SELECT 
    rating_kp,
    votes_kp
FROM movies
WHERE rating_kp > 0
ORDER BY rating_kp;

SELECT *
FROM rating_votes_kp

--6
CREATE VIEW rating_votes_imdb AS
SELECT 
    rating_imdb,
    votes_imdb
FROM movies
WHERE rating_imdb > 0
ORDER BY rating_imdb;

SELECT *
FROM rating_votes_imdb

--7
CREATE VIEW avg_rating_by_genre AS
SELECT 
    country,
    COUNT(*) AS movies_count,
    ROUND(AVG(rating_kp), 2) AS avg_rating_kp,
    ROUND(AVG(rating_imdb), 2) AS avg_rating_imdb
FROM movies
WHERE rating_kp > 0 AND rating_imdb> 0
GROUP BY country
HAVING COUNT(*) > 5
ORDER BY movies_count DESC;

SELECT *
FROM avg_rating_by_genre

--8
CREATE OR REPLACE VIEW fees_details AS
SELECT 
    id,
    title,
    country,
    genres,
    age_rating,
    votes_kp,
    votes_imdb,
    rating_kp,
    rating_imdb,
    fees_world_value,
    fees_usa_value,
    fees_russia_value,
    fees_sum
FROM movies
WHERE fees_sum IS NOT NULL 
  AND rating_kp > 0 
  AND rating_imdb > 0
ORDER BY fees_sum DESC;

SELECT *
FROM fees_details

SELECT column_name, data_type 
FROM information_schema.columns 
WHERE table_name = 'movies';

SELECT * FROM public.movies LIMIT 5;

ROLLBACK;

SELECT SUM(fees_world_value) 
FROM public.movies 
WHERE fees_world_value IS NOT NULL;

SELECT * FROM pg_stat_activity

SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE application_name LIKE 'DBeaver%'
  AND pid <> pg_backend_pid();

SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE state != 'idle'
  AND pid <> pg_backend_pid();

SELECT current_user, session_user;

SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE state != 'idle'
  AND pid <> pg_backend_pid();

SELECT current_user, session_user;

SELECT usesuper
FROM pg_user
WHERE usename = current_user;

SELECT *
from movies

ALTER TABLE movies ADD COLUMN first_genre TEXT;

UPDATE movies
SET first_genre = genres[1];

ALTER TABLE movies ADD COLUMN first_country TEXT;

UPDATE movies
SET first_country = split_part(country, ', ', 1);

select distinct first_country
from movies

select distinct first_genre
from movies

SELECT COUNT(*) FROM movies;

