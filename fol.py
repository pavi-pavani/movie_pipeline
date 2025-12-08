# import pandas as pd
# import sqlalchemy
# import pymysql
# import requests
# from dotenv import load_dotenv

# print("All good!")

# import MySQLdb
# print("mysqlclient is working!")

import requests, os, urllib.parse
from dotenv import load_dotenv

load_dotenv()
title = "Toy Story (1995)"
api_key = os.getenv("OMDB_API_KEY")
encoded_title = urllib.parse.quote(title)
url = f"http://www.omdbapi.com/?t={encoded_title}&apikey={api_key}"
data = requests.get(url).json()
print(data)



