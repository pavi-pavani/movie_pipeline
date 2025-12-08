# Movie Pipeline ETL Project

## Table of Contents

* [Overview](#overview)
* [Tech Stack](#tech-stack)
* [Folder Structure](#folder-structure)
* [Setup](#setup)
* [ETL Pipeline](#etl-pipeline)
* [Database Schema](#database-schema)
* [Queries](#queries)
* [Design Choices](#design-choices)
* [Challenges](#challenges)
* [License](#license)

---

## Overview

This project implements an end-to-end **ETL (Extract, Transform, Load) pipeline** for the MovieLens dataset using Python, MySQL, and SQL.
It enriches the dataset with additional metadata from the OMDb API and loads cleaned and structured data into a relational database.

The project demonstrates skills in:

* Data extraction, transformation, and loading
* API integration
* SQL database design and queries
* Python programming for data engineering tasks

---

## Tech Stack

* Python 3.x
* Pandas
* SQLAlchemy
* MySQL / MariaDB
* OMDb API
* VS Code

---

## Folder Structure

```
movie_pipeline/
├── etl.py               # Main ETL script
├── schema.sql           # SQL to create database schema
├── queries.sql          # Analytical SQL queries
├── README.md            # Project documentation
├── requirements.txt     # Python dependencies
├── .gitignore           # Git ignore rules
├── .venv/               # Local virtual environment (ignored)
├── data/                # Raw data & cache (ignored)
│   ├── movies.dat
│   ├── ratings.dat
│   └── omdb_cache.csv
└── .env                 # Environment variables (ignored)
```

---

## Setup

1. **Clone repository:**

```bash
git clone https://github.com/pavi-pavani/movie_pipeline.git
cd movie_pipeline
```

2. **Create virtual environment:**

```bash
python -m venv .venv
```

3. **Activate virtual environment:**

* Windows:

```bash
.venv\Scripts\activate
```

* Mac/Linux:

```bash
source .venv/bin/activate
```

4. **Install dependencies:**

```bash
pip install -r requirements.txt
```

5. **Set up environment variables:**
   Create a `.env` file with your credentials:

```
OMDB_API_KEY=your_omdb_api_key
DATABASE_URL=mysql+mysqlconnector://username:password@localhost:3306/moviedb
```

---

## ETL Pipeline

**`etl.py`** performs the following steps:

1. Load MovieLens `movies.dat` and `ratings.dat`.
2. Fetch additional movie metadata from the OMDb API (or load cached CSV).
3. Clean and transform data:

   * Extract year from title and normalize genres
   * Handle missing values
   * Convert box office and IMDb ratings to numeric
4. Create database tables:

   * `movies`, `genres`, `movie_genres`, `ratings`
5. Load data into MySQL using SQLAlchemy.

---

## Database Schema

* **movies**:
  `movieid`, `title`, `year`, `director`, `plot`, `boxoffice`, `imdbrating`, `released`

* **genres**:
  `genre_id`, `genre`

* **movie_genres**:
  `movieid`, `genre_id` (many-to-many mapping)

* **ratings**:
  `rating_id`, `userid`, `movieid`, `rating`, `timestamp`

*(Refer to `schema.sql` for full SQL definitions.)*

---

## Queries

Analytical SQL queries are saved in **`queries.sql`**, including:

1. Highest rated movie
2.  Top 5 genres by average rating
3.  Who is the director with the most movies in this dataset
4 . What is the average rating of movies released each year

---

## Design Choices

* Normalized database with separate `genres` and `movie_genres` tables
* Cached OMDb API data to avoid repeated API calls
* Cleaned and converted all numeric and date fields for consistency
* Modular ETL code to allow easy re-run and expansion

---

## Challenges

* Handling missing or inconsistent data in MovieLens and OMDb API
* Converting box office and IMDb ratings to numeric values
* Avoiding API overuse with caching
* Managing many-to-many genre mapping in SQL
* By joining tables foreign key issues
* Handling missing values and nulls in the data
* Avoiding duplicate movie entries during ETL

---

## License

This project is for **educational and interview purposes only**.




