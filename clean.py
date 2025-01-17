"""This is a script that cleans Titles table in netflix-data CSV file."""
import psycopg2
from config import load_config

def connect(config):
    try: 
        # 'with' automatically closes the connection when done
        with psycopg2.connect(**config) as conn:
            with conn.cursor() as cursor:
                # update title column to clean up the show name
                title_update = """
                UPDATE Titles 
                SET title = REGEXP_REPLACE(title, '(:?\\s*:?\\s*(Season|Episode|Parts|Chapter)[^\\)]*)|(\\s*\\(.*\\))', '', 'g');"""

                cursor.execute(title_update)
                conn.commit()
                print("Updated title field successfully!")
        
    except(psycopg2.DatabaseError, Exception) as E:
        print(E)

if __name__ == "__main__":
    config = load_config()
    connect(config)