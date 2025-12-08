# Movie Analysis Pipeline

A project to extract, transform, and load movie data, and perform analysis on top-rated movies and genres.

---

## Table of Contents
- [Overview](#overview)
- [Setup](#setup)
- [Usage](#usage)
- [Database Schema](#database-schema)
- [Queries](#queries)
- [Design Choices](#design-choices)
- [Challenges](#challenges)
- [License](#license)

---

## Overview
This project builds a simple movie analytics pipeline. It performs the following:
1. Loads movie, user, and ratings data into a MySQL database.
2. Performs SQL queries to find highest-rated movies and top genres.
3. Can be extended to include visualizations or advanced analytics.

---

## Setup

1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/your-repo-name.git
   cd your-repo-name


2. Install dependencies:
   pip install -r requirements.txt

3. Create database tables:
   mysql -u your_user -p movie_db < schema.sql

4. Run the ETL script:
   python etl.py


## Usage
After running the ETL script, your database will be populated.  
Run the SQL queries in `queries.sql` to find:
- Highest rated movie
- Top 5 genres by average rating
- Who is the director with the most movies in this dataset
- What is the average rating of movies released each year

## Database Schema

This project uses a MySQL database with the following tables:

| Table   | Columns                                      | Description |
|---------|---------------------------------------------|-------------|
| movies  | movieId (PK), title, genre                  | Stores movie information, including title and genre. |
| users   | userId (PK), username, email                | Stores user information. |
| ratings | ratingId (PK), userId (FK), movieId (FK), rating, timestamp | Stores user ratings for movies, linking users and movies with foreign keys. |

# Notes
- **Primary Keys (PK)** ensure each record is unique.  
- **Foreign Keys (FK)** enforce relationships between tables (users → ratings, movies → ratings).  
- **Data types** are chosen to optimize storage and maintain data integrity (e.g., rating INT 1-5).  


## Queries
- Find the highest rated movie
- Find the top 5 genres by average rating


## Design Choices
- Ratings are integers from 1 to 5
- Foreign keys maintain relationships between tables
- Handled duplicates during ETL


## Challenges
- Handling missing values and nulls in the data
- Avoiding duplicate movie entries during ETL
- by joining tables foreign key issues

## License
This project is for demonstration purposes and personal portfolio use.





