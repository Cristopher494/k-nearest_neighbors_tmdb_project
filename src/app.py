from utils import db_connect
import pandas as pd
import psycopg2

# Configurar la conexión a la base de datos
database_username = 'postgres'
database_password = 'Dinamarca1'
database_ip = 'localhost'
database_name = 'my_db'
database_port = '5432'

# Crear la cadena de conexión
connection = psycopg2.connect(
    host=database_ip,
    port=database_port,
    database=database_name,
    user=database_username,
    password=database_password
)

engine = db_connect()

engine
if engine:
    print("Conexión exitosa")

movies = pd.read_csv('https://raw.githubusercontent.com/4GeeksAcademy/k-nearest-neighbors-project-tutorial/main/tmdb_5000_movies.csv')
movies.to_sql('movies_df', engine, if_exists='replace', index=False)

credits = pd.read_csv('https://raw.githubusercontent.com/4GeeksAcademy/k-nearest-neighbors-project-tutorial/main/tmdb_5000_credits.csv')
credits.to_sql('credits_df', engine, if_exists='replace', index=False)

# Unir las dos tablas por el título
query = """
SELECT *
FROM movies_df AS movies
JOIN credits_df AS credits
ON movies.title = credits.title
"""