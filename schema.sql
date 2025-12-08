-- Movies Table
CREATE TABLE movies (
    movieid INT AUTO_INCREMENT PRIMARY KEY,
    imdb_id VARCHAR(20) UNIQUE,
    title VARCHAR(255) NOT NULL,
    release_date DATE,
    director VARCHAR(255),
    plot TEXT,
    box_office BIGINT
);

-- Genres Table
CREATE TABLE genres (
    genre_id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100) UNIQUE
);

-- Movie_Genres Table
CREATE TABLE movie_genres (
    movieid INT,
    genre_id INT,
    PRIMARY KEY (movieid, genre_id),
    FOREIGN KEY (movieid) REFERENCES movies(movieid) ON DELETE CASCADE,
    FOREIGN KEY (genre_id) REFERENCES genres(genre_id) ON DELETE CASCADE
);

-- Ratings Table
CREATE TABLE ratings (
    rating_id INT AUTO_INCREMENT PRIMARY KEY,
    userid INT NOT NULL,
    movieid INT,
    rating FLOAT,
    timestamp DATETIME,
    FOREIGN KEY (movieid) REFERENCES movies(movieid) ON DELETE CASCADE
);


