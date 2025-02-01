"""Script that populates Genres table"""
import json
import psycopg2
from config import load_config


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
    

def delete_unwanted_genres(config: dict):
   try:
      unwanted_genres = [
         "Western", "Action & Adventure", "Animation", "Comedy", "Crime", "Documentary", 
         "Drama", "Family", "Kids", "Mystery", "News", "Reality", "Sci-Fi & Fantasy", 
         "Soap", "Talk", "War & Politics"
      ]

      with psycopg2.connect(**config) as conn:
         with conn.cursor() as cursor:
            for genre in unwanted_genres:
               cursor.execute(""" 
                              DELETE FROM Genres WHERE genre = %s;
                              """, (genre,))
            conn.commit()
            print("Successfully deleted unwanted genres from TMDB.")
   except Exception as e:
      print(f"Could not delete unwanted genres: {e}")

    
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


def main():
   config = load_config('database.ini')
   file_path = 'genres.json'
   delete_unwanted_genres(config)
   insert_genre(file_path, config)
   print("Data from JSON inserted successfully!")

if __name__ == "__main__":
   main()