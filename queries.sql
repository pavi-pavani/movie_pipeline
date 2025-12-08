
-- 1. Which movie has the highest average rating?
SELECT 
    m.title,
    AVG(r.rating) AS avg_rating
FROM movies m
JOIN ratings r ON m.movieId = r.movieId
GROUP BY m.movieId, m.title
ORDER BY avg_rating DESC
LIMIT 1;


-- 2. Top 5 genres with the highest average rating
SELECT 
    g.genre AS genre_name,
    AVG(r.rating) AS avg_rating
FROM movie_genres mg
JOIN genres g ON mg.genre_id = g.genre_id
JOIN ratings r ON mg.movieid = r.movieId
GROUP BY g.genre
ORDER BY avg_rating DESC
LIMIT 5;

---3. Who is the director with the most movies in this dataset
SELECT 
    director,
    COUNT(*) AS movie_count
FROM movies
GROUP BY director
ORDER BY movie_count DESC
LIMIT 1;

--4. What is the average rating of movies released each year?
SELECT 
    m.year AS release_year,
    AVG(r.rating) AS avg_rating
FROM movies m
JOIN ratings r ON m.movieId = r.movieId
GROUP BY m.year
ORDER BY release_year;


