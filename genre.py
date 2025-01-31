"""This script uses the OMDB API to get the genres for each title in Title table"""
import requests
import psycopg2
import json
from urllib.parse import quote_plus
from config import load_config
from requests.exceptions import RequestException


def connect(config):
  """Connect to database"""
  try:
    with psycopg2.connect(**config) as conn:
      print('Connected to the PostgreSQL server.')
      return conn
  except (psycopg2.DatabaseError, Exception) as error:
    print(error)
    

def load_genres_from_json(file_path):
    """Load genres from a local JSON file."""
    try:
        # Open the JSON file and load its contents into a Python object (usually a list or dict)
        with open(file_path, 'r') as file:
            genres_data = json.load(file)
            return genres_data
    except Exception as e:
        print(f"Error loading genres from file: {e}")
        return []
    
def load_api_keys(file_path):
    """Load api keys"""
    try:
        with open(file_path, 'r') as file:
            api_keys = json.load(file)
            return api_keys
    except Exception as e:
        print(f"Error loading keys from file: {e}")
        return {}
    
def clean_genre_data(raw_genre_data):
    """Sanitize the genre list and return a cleaned list"""
    # Remove any unwanted spaces and commas
    cleaned_genre_list = []
    
    # Split by commas if the genre data is a string
    if isinstance(raw_genre_data, str):
        # Split by commas, strip spaces, and remove any empty strings
        cleaned_genre_list = [genre.strip() for genre in raw_genre_data.split(',') if genre.strip()]
    elif isinstance(raw_genre_data, list):
        # If it's already a list, just strip and remove empty items
        cleaned_genre_list = [genre.strip() for genre in raw_genre_data if genre.strip()]

    # Check for any invalid data like non-ASCII characters, or malformed data
    cleaned_genre_list = [genre for genre in cleaned_genre_list if genre.isprintable()]

    # Return the cleaned list
    return cleaned_genre_list


def get_omdb_genre(title: str, api_key: str):
   """Get the title genre using the OMDB API"""
   try:
      encoded_title = quote_plus(title)
      print(f"Fetching OMDB ID for '{title}'...")
      
      url = f"http://www.omdbapi.com/?apikey={api_key}&t={encoded_title}"
      #else:
         # Make sure you are searching for a specific TV show
         #url = f"https://api.themoviedb.org/3/search/tv?api_key={api_key}&query={title}"

      response = requests.get(url)

      if response.status_code == 200:
        data = response.json()
        
        if data.get("Response") == "True":
            genre = data.get("Genre", "")
            # Split the genre string into a list if it's available
            genre_list = genre.split(", ") if genre else []
            return genre_list
        else:
            print(f"Movie not found: {title}")
            return []
      else:
        print(f"Failed to retrieve data for {title}")
        return []

   except RequestException as e:
       print(f"Error fetching data from OMDB for {title}: {e}")
       return []

'''
def get_genres(title: str, video_type: str):
    #Gets all genres to populate Genres table
    try:
        if video_type == 'movie':
         url = f" https://api.themoviedb.org/3/genre/movie/list?api_key={t_api_key}&language=en"
        else:
         url = f"https://api.themoviedb.org/3/genre/tv/list?api_key={t_api_key}&language=en"

        response = requests.get(url)
        response.raise_for_status() # raises either 4xx or 5xx errors

        data = response.json() # parses the JSON responses
        # This gets the data(value) associated with the key 'genres' and if none, then an empty list
        # looks like a list of multiple dicts, i.e {id: 0, name: animation}
        all_genres = data.get('genres', [])
        return all_genres
    
    except RequestException as e:
       print(f"Error fetching data from TMDB for {title}: {e}")
       return []
    except ValueError as e:
       print(f"Could not parse JSON for {title}: {e}")
       return []
'''

def insert_genre(file_path, config: dict):
    """Inserts the genres fetched into Genres table if it doesn't already exist"""
    try:
      # Open the JSON file and load its contents into a Python object (usually a list or dict)
      genres_data = load_genres_from_json(file_path)
      

      # Connect to database and insert genres into Genre table
      with psycopg2.connect(**config) as conn:
         with conn.cursor() as cursor:
            # use %s as placeholders in psycopg2
            for genre_name in genres_data:
               if genre_name:
                  cursor.execute("""
                     INSERT INTO Genres (genre)
                     VALUES (%s)
                     ON CONFLICT (genre) DO NOTHING;
                  """, (genre_name,)) # this is what will be substituted in %s from get_genres

            conn.commit()
            print("Added genres to Genres table successfully.")      
    
    except Exception as e:
        print(f"Error inserting genres into Genres: {e}")
    except Exception as e:
        print(f"Error loading genres from file: {e}")


def update_title_genre(genre_list: list, title:str, config: dict):
   """Updates the genre(s) for a specific title in Titles table"""
   """genres_data stores the output of genres variable"""
   try:
      
      if not genre_list:
            print(f"No genres found for {title}, skipping update.")
            genre_value = None
      
      cleaned_genre_list = clean_genre_data(genre_list)

      with psycopg2.connect(**config) as conn:
         with conn.cursor() as cursor:
               genre_value = ", ".join(cleaned_genre_list) if cleaned_genre_list else None
               
               cursor.execute("""
                  UPDATE Titles
                  SET genre = %s 
                  WHERE title = %s;
               """, (genre_value, title))
               conn.commit()
               print(f"Genres updated for {title}")
                     
   except Exception as e:
      print(f"Error updating genre(s) for {title}: {e}")

   
"""Batch processing using update_title_genre for all titles in Titles"""
def update_all(config: dict, genre_file_path, omdb):
   try:
      
      insert_genre(genre_file_path, config)

      with psycopg2.connect(**config) as conn:
         with conn.cursor() as cursor:
            # Get all titles from table
            cursor.execute("SELECT title, video_type FROM Titles")
            titles = cursor.fetchall()

            # Iterate through each title
            for title in titles:
               title_name = title[0]
               
               print(f"Updating genres for '{title_name}'...")

               # Fetch genres for the title using the OMDb API
               genre_names = get_omdb_genre(title_name, omdb)
                    
               if genre_names:
                  # Update the title's genres in the Titles table
                  update_title_genre(genre_names, title_name, config)
                  print(f"Genres for '{title_name}': {genre_names}")
               else:
                  
                  print(f"No genres found for '{title_name}'. Skipping...")

   except Exception as e:
      print(f"Error fetching titles or updating genres: {e}")




def main():
   config = load_config('database.ini')
   conn = connect(config)
   genre_file_path = 'genres.json'
   keys_file_path = 'keys.json'

   api_keys = load_api_keys(keys_file_path)
   omdb = api_keys.get('omdb')

   insert_genre(genre_file_path, config)

   if conn:
      update_all(config, genre_file_path, omdb)
   else:
      print("Failed to connect to the database.")

if __name__ == "__main__":
   main()