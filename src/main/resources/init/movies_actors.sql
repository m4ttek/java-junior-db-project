CREATE TABLE IF NOT EXISTS movies_actors (
    movie_id BIGINT NOT NULL,
    actor_id BIGINT NOT NULL,
    character VARCHAR(1000) NOT NULL,

    FOREIGN KEY (movie_id) REFERENCES movies(id),
    FOREIGN KEY (actor_id) REFERENCES actors(id)
);