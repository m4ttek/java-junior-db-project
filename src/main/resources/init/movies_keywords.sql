CREATE TABLE IF NOT EXISTS movies_keywords (
    movie_id BIGINT NOT NULL,
    keyword_id BIGINT NOT NULL,

    FOREIGN KEY (movie_id) REFERENCES movies(id),
    FOREIGN KEY (keyword_id) REFERENCES keywords(id)
);