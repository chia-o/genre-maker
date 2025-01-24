"""This script uses the TMDB API to get the genres for each title in Title table"""
import requests
import psycopg2
from config import load_config
from requests.exceptions import RequestException

movie_api_url = "https://api.themoviedb.org/3/genre/movie/list"
tv_api_url = "https://api.themoviedb.org/3/genre/tv/list"
 = ''

headers = {
    "accept": 'application/json',
    "Authorization": f'Bearer {}'
}

"""Connect to database"""
def connect(config):
  try:
    with psycopg2.connect(**config) as conn:
      print('Connected to the PostgreSQL server.')
      return conn
  except (psycopg2.DatabaseError, Exception) as error:
    print(error)

"""Gets the genres for movies and tv shows in Title"""
def get_genre(title_id: int, video_type: str):
    try:
        if video_type == 'movie':
            url = f"{movie_api_url}?={}&language=en"
        else:
            url = f"{tv_api_url}?={}&language=en"

        response = requests.get(url)
        response.raise_for_status() # raises either 4xx or 5xx errors

        data = response.json() # parses the JSON responses
        # This is supposed to return the genres as a list, but not sure how
        genres = data.get('genres', [])
        return genres
    
    except RequestException as e:
       print(f"Error fetching data from TMDB for {title_id}: {e}")
       return []
    except ValueError as e:
       print(f"Could not parse JSON for {title_id}: {e}")
       return []


"""Inserts the genres fetched into Genres table if it doesn't already exist"""
def insert_genre(genre: str, genre_id: int, config: dict):
    try:
      with psycopg2.connect(**config) as conn:
         with conn.cursor() as cursor:
            # use %s as placeholders in psycopg2
            cursor.execute("""
               INSERT INTO Genres (genre, genre_id)
               VALUES (%s, %s)
               ON CONFLICT (genre_id) DO NOTHING;
            """, (genre, genre_id)) # this is what will be substituted in %s       
    except Exception as e:
        print(f"Error inserting {genre} into Genres: {e}")


"""Updates the genre(s) for a specific title in Titles table"""
"""genres_data stores the output of genres variable"""
def update_title_genre(title_id: int, genres_data: list, config: dict):
   try:
      with psycopg2.connect(**config) as conn:
         with conn.cursor() as cursor:
            # initialize list to store genres
            genre_names = []

            for genre in genres_data:
               genre = genre['name']
               # append a genre to list
               genre_names.append(genre)

            # update Titles table with genres
            cursor.execute("""
               UPDATE Titles
               SET genre = %s 
               WHERE title_id = %s;
               """, (genre_names, title_id))
               
   except Exception as e:
      print(f"Error updating genre(s) for {title_id}: {e}")

   
"""Batch processing using update_title_genre for all titles in Titles"""
def update_all(config: dict):
   try:
      with psycopg2.connect(**config) as conn:
         with conn.cursor() as cursor:
            # Get all titles from table
            cursor.execute("SELECT title_id, video_type FROM Titles")
            titles = cursor.fetchall()

            for title in titles:
               # this sets id to first item in tuple from table, title_id
               title_id = title[0]
               print(f"Updating genres for {title_id}")

               # second item = 
               video_type = title[2]
               if video_type == "movie":
                  genres = get_genre(title_id, 'movie')
                  update_title_genre(title_id, genres, config)
               else:
                  genres = get_genre(title_id, 'tv')
                  update_title_genre(title_id, genres, config)


   except Exception as e:
      print(f"Error fetching titles or updating genres: {e}")
