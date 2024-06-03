import pandas as pd
from sqlalchemy import create_engine, text
import os
import psycopg2
import json

url_movies = 'https://raw.githubusercontent.com/4GeeksAcademy/k-nearest-neighbors-project-tutorial/main/tmdb_5000_movies.csv'
url_credits = 'https://raw.githubusercontent.com/4GeeksAcademy/k-nearest-neighbors-project-tutorial/main/tmdb_5000_credits.csv'

df_movies = pd.read_csv(url_movies)
df_credits = pd.read_csv(url_credits)


# Configurar la conexión a la base de datos
database_username = 'postgres'
database_password = 'Dinamarca1'
database_ip = 'localhost'
database_name = 'my_db'
database_port = '5432'

# Conectar a la base de datos PostgreSQL
try:
    connection = psycopg2.connect(
    host=database_ip,
    port=database_port,
    database=database_name,
    user=database_username,
    password=database_password
)
    connection.autocommit = True
except psycopg2.Error as e:
    print(f"Error al conectar a la base de datos: {e}")
    raise

# Definir las columnas que queremos seleccionar
columns = [
    'credits.title',
    'credits.movie_id',
    'credits.cast',
    'credits.crew',
    'movies.overview', 
    'movies.genres',
    'movies.keywords'
]

# Crear la tabla new_table
query_create = f'''
CREATE TABLE new_table AS
SELECT {', '.join(columns)}
FROM credits
INNER JOIN movies ON credits.title = movies.title;
'''

# Ejecutar la consulta SQL para crear la nueva tabla
try:
    with connection.cursor() as cursor:
        cursor.execute(query_create)
except psycopg2.Error as e:
    print(f"Error al crear la tabla: {e}")
    raise


query = '''
SELECT title, movie_id, cast, crew, overview, genres, keywords
FROM new_table;
'''

from sqlalchemy import create_engine

# Crear una conexión SQLAlchemy
engine = create_engine('postgresql://user:password@localhost:5432/database')

# Ejecutar la consulta SQL y cargar los datos en un DataFrame de Pandas
data_films = pd.read_sql_query(query, engine)

# Definir el directorio de salida
output_dir = '../data/interim'

# Crear el directorio si no existe
os.makedirs(output_dir, exist_ok=True)

# Guardar el DataFrame en un archivo CSV
output_path = os.path.join(output_dir, 'data_films.csv')
data_films.to_csv(output_path, index=False)
print(f"Archivo guardado en {output_path}")

# Cerrar la conexión a la base de datos
engine.dispose()
