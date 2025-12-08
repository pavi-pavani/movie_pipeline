import pandas as pd
import requests
from sqlalchemy import create_engine, text   # ✅ Add `text` here
from dotenv import load_dotenv
import os
import time
import warnings


warnings.filterwarnings("ignore", category=FutureWarning)

#1 Load environment variables
load_dotenv()
OMDB_API_KEY = os.getenv("OMDB_API_KEY")
DATABASE_URL = os.getenv("DATABASE_URL")

# Create SQLAlchemy engine
engine = create_engine(DATABASE_URL)





# 2 load the movies and rating.dat file
movies_path = r"D:\project\movie_pipeline\data\movies.dat"
ratings_path = r"D:\project\movie_pipeline\data\ratings.dat"

import os

print(os.path.exists(movies_path))   # Should be True
print(os.path.exists(ratings_path))  # Should be True



movies_df = pd.read_csv(movies_path, sep="::", engine='python', header=None, names=["movieId", "title", "genres"],encoding="latin-1")
ratings_df = pd.read_csv(ratings_path, sep="::", engine='python', header=None, names=["userId", "movieId", "rating", "timestamp"],encoding="latin-1")


print(movies_df.head())
print(ratings_df.head())


# 3 Fetch additional movie details from OMDb
# -----------------------------
# Function: Fetch OMDb details
# -----------------------------
def fetch_omdb_details(title):
    """Fetch details for one movie from the OMDb API."""
    import urllib.parse
    import requests
    import os

    try:
        # Extract title and year separately
        import re
        match = re.match(r"^(.*)\s\((\d{4})\)$", title.strip())
        if match:
            clean_title, year = match.groups()
        else:
            clean_title, year = title, ""

        encoded_title = urllib.parse.quote(clean_title)
        api_key = os.getenv("OMDB_API_KEY")

        # Use both title and year for better accuracy
        url = f"http://www.omdbapi.com/?t={encoded_title}&y={year}&apikey={api_key}"

        response = requests.get(url)
        data = response.json()

        if data.get("Response") == "True":
            return {
                'title': title,
                'director': data.get('Director'),
                'plot': data.get('Plot'),
                'box_office': data.get('BoxOffice'),
                'imdbrating': data.get('imdbRating'),
                'released': data.get('Released')
            }
        else:
            print(f"Not found: {title}")
            return {
                'title': title,
                'director': None,
                'plot': None,
                'box_office': None,
                'imdbrating': None,
                'released': None
            }

    except Exception as e:
        print(f"Error fetching {title}: {e}")
        return {
            'title': title,
            'director': None,
            'plot': None,
            'box_office': None,
            'imdbrating': None,
            'released': None
        }



#  Step 4: Try loading cached data first (to avoid re-fetching)
# Step 4: Fetch OMDb data or load cache
os.makedirs("data", exist_ok=True)
cache_path = "data/omdb_cache.csv"

if os.path.exists(cache_path):
    print("Loading cached OMDb data...")
    api_df = pd.read_csv(cache_path)
else:
    print("Fetching fresh data from OMDb...")
    api_results = []
    for title in movies_df['title']:
        details = fetch_omdb_details(title)
        api_results.append(details)
        time.sleep(1)
    api_df = pd.DataFrame(api_results)
    api_df.to_csv(cache_path, index=False)
    print("OMDb data cached at data/omdb_cache.csv")

print("API data sample:\n", api_df.head())

# Clean up api_df column names and ensure match
api_df.columns = api_df.columns.str.strip().str.lower()
if 'title' not in api_df.columns and 'Title' in api_df.columns:
    api_df = api_df.rename(columns={'Title': 'title'})

# Merge correctly (only once!)
movies_df = movies_df.merge(api_df, on='title', how='left')




# 5 Handle missing values & data types(transform data)

#movies data

#Strip whitespace & special characters
movies_df['title'] = movies_df['title'].str.strip()
#Handle missing titles
movies_df['title'] = movies_df['title'].fillna('Unknown')
#extract year from title
movies_df['year'] = movies_df['title'].str.extract(r'\((\d{4})\)')
movies_df['year'] = pd.to_numeric(movies_df['year'], errors='coerce')
#Split genres into list
movies_df['genres_list'] = movies_df['genres'].apply(lambda x: x.split('|') if pd.notnull(x) else [])
#Normalize genres
movies_df['genres_list'] = movies_df['genres_list'].apply(lambda g: list(set([x.strip() for x in g])))
#Fill missing genres
movies_df['genres_list'] = movies_df['genres_list'].apply(lambda g: g if g else ['Unknown'])
#decade
movies_df['decade'] = (movies_df['year'] // 10) * 10


#ratings data

#Convert types
ratings_df['userId'] = pd.to_numeric(ratings_df['userId'], errors='coerce')
ratings_df['movieId'] = pd.to_numeric(ratings_df['movieId'], errors='coerce')
ratings_df['rating'] = pd.to_numeric(ratings_df['rating'], errors='coerce')
ratings_df['timestamp'] = pd.to_datetime(ratings_df['timestamp'], unit='s', errors='coerce')
#Handle missing values
ratings_df = ratings_df.dropna(subset=['userId', 'movieId', 'rating'])
#Remove duplicates
ratings_df = ratings_df.drop_duplicates(subset=['userId','movieId'], keep='last')


# Clean up column names just once (good habit for all DataFrames)

# Then safely fill missing values
movies_df.columns = movies_df.columns.str.strip().str.lower()

#Fetch additional columns: director, plot, box_office, imdbRating, Released
movies_df['director'] = movies_df.get('director', pd.Series('Unknown')).fillna('Unknown')
movies_df['plot'] = movies_df.get('plot', pd.Series('No plot available')).fillna('No plot available')

# Clean column names once
movies_df.columns = movies_df.columns.str.strip().str.lower()

# Clean and convert box_office column safely



if 'boxoffice' in movies_df.columns:
    movies_df['boxoffice'] = pd.to_numeric(
        movies_df['boxoffice']
        .astype(str)                             # ensure it's a string
        .str.replace(r'[\$,]', '', regex=True),  # remove $ and ,
        errors='coerce'
    ).fillna(0)
else:
    # if column missing, create one filled with zeros
    movies_df['boxoffice'] = 0


if 'box_office' in movies_df.columns and 'boxoffice' in movies_df.columns:
    # Merge values
    movies_df['boxoffice'] = movies_df['boxoffice'].combine_first(movies_df['box_office'])
    # Drop the old column without inplace
    movies_df = movies_df.drop(columns=['box_office'])
    

# Normalize column names first
movies_df.columns = movies_df.columns.str.strip().str.lower()

# Convert imdbRating → numeric
if 'imdbrating' in movies_df.columns:
    movies_df['imdbrating'] = pd.to_numeric(movies_df['imdbrating'], errors='coerce').fillna(0)
else:
    movies_df['imdbrating'] = 0  # default if missing

# Convert Released → datetime
if 'released' in movies_df.columns:
    movies_df['released'] = pd.to_datetime(movies_df['released'], errors='coerce')
else:
    movies_df['released'] = pd.NaT  # default if missing


# Load DataFrames to MySQL (Idempotent)
# 6 to_sql with if_exists='replace' or append 


# 1️ Movies table


movies_final_df = movies_df[[
    'movieid', 'title', 'year', 'director', 'plot', 'boxoffice', 'imdbrating', 'released'
]]

from sqlalchemy.types import Integer, String, BigInteger, Float, DateTime, Text


with engine.begin() as conn:
    # Temporarily disable foreign key checks
    conn.execute(text("SET FOREIGN_KEY_CHECKS = 0;"))

    movies_final_df = movies_final_df.loc[:, ~movies_final_df.columns.duplicated()]

    
    # Drop and recreate safely              #to the my sql 
    movies_final_df.to_sql(
    'movies',
    con=conn,
    if_exists='replace',
    index=False,
    dtype={
        'movieid': Integer(),
        'title': String(255),
        'year': Integer(),
        'director': String(255),
        'plot': Text(),
        'boxoffice': BigInteger(),
        'imdbrating': Float(),
        'released': DateTime()
    }
)


    
    # Re-enable checks
    conn.execute(text("SET FOREIGN_KEY_CHECKS = 1;"))



# 2️ Genres table
# Get unique genres
genres_list = set(g for sublist in movies_df['genres_list'] for g in sublist)
genres_df = pd.DataFrame({'genre': list(genres_list)})
genres_df['genre_id'] = range(1, len(genres_df)+1)

# to the sql 
genres_df.to_sql(
    'genres', con=engine, if_exists='replace', index=False,
    dtype={'genre_id': Integer(), 'genre': String(100)}
)


# 3️ Movie-Genres mapping table
movie_genres_df = movies_df[['movieid', 'genres_list']].explode('genres_list')
movie_genres_df = movie_genres_df.merge(genres_df, left_on='genres_list', right_on='genre', how='left')
movie_genres_df = movie_genres_df[['movieid', 'genre_id']]

#to the sql
movie_genres_df.to_sql(
    'movie_genres', con=engine, if_exists='replace', index=False,
    dtype={'movieid': Integer(), 'genre_id': Integer()}
)


# 4️ Ratings table  # to the sql

# Add unique rating_id before loading
ratings_df = ratings_df.reset_index(drop=True)
ratings_df['rating_id'] = ratings_df.index + 1


ratings_df.to_sql(
    'ratings', con=engine, if_exists='replace', index=False,
    dtype={
        'rating_id': Integer(),
        'userid': Integer(),
        'movieid': Integer(),
        'rating': Float(),
        'timestamp': DateTime()
    }
)


print(" All tables loaded into the database successfully!")




























