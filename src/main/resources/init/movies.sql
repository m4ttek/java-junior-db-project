CREATE TABLE IF NOT EXISTS movies (
    id BIGINT PRIMARY KEY,
    original_language CHAR(2),
    overview TEXT,
    popularity DOUBLE,
    release_date DATE,
    title VARCHAR(255),
    vote_average DOUBLE,
    vote_count LONG,
    genres VARCHAR(100) ARRAY
);
