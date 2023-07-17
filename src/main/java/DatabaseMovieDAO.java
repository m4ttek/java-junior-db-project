import java.sql.Array;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class DatabaseMovieDAO implements MoviesDAO {

    private final Connection dbConnection;

    public DatabaseMovieDAO(Connection dbConnection) {
        this.dbConnection = dbConnection;
    }

    @Override
    public void save(Movie movie) {
        if (movie.id() != null) {
            throw new MovieDAOException("Movie object should not contain any id when used in save method!");
        }
        try (var movieInsertStatement = dbConnection.prepareStatement("""
                INSERT INTO movies (original_language, overview, popularity, release_date, title, vote_average, vote_count, genres)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """)) {

            mapMovieToStatement(movieInsertStatement, movie);
            if (movieInsertStatement.executeUpdate() != 1) {
                throw new MovieDAOException("Could not insert a new movie!");
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void update(Movie movie) {
        if (movie.id() == null) {
            throw new MovieDAOException("Movie object must contain id when used in update method!");
        }
        try (var movieUpdateStatement = dbConnection.prepareStatement("""
                UPDATE movies
                SET original_language = ?, overview = ?, popularity = ?, release_date = ?, title = ?, vote_average = ?, vote_count = ?, genres = ?
                WHERE id = ?
                """)) {

            mapMovieToStatement(movieUpdateStatement, movie);
            movieUpdateStatement.setLong(9, movie.id());
            if (movieUpdateStatement.executeUpdate() != 1) {
                throw new MovieDAOException("Could not update movie with id: " + movie.id());
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void delete(Movie movie) {
        if (movie.id() == null) {
            throw new MovieDAOException("Movie object must contain id when used in delete method!");
        }

        try (var movieDeleteStatement = dbConnection.prepareStatement("""
                DELETE FROM movies
                WHERE id = ?;
                """);
            var actorsRelatedStatement = dbConnection.prepareStatement("""
                DELETE FROM movies_actors
                WHERE movie_id = ?
                """);
             var keywordsRelatedStatement = dbConnection.prepareStatement("""
                DELETE FROM movies_keywords
                WHERE movie_id = ?
                """)) {

            actorsRelatedStatement.setLong(1, movie.id());
            actorsRelatedStatement.executeUpdate();

            keywordsRelatedStatement.setLong(1, movie.id());
            keywordsRelatedStatement.executeUpdate();

            movieDeleteStatement.setLong(1, movie.id());
            if (movieDeleteStatement.executeUpdate() != 1) {
                throw new MovieDAOException("No deletion happened for movie with id: " + movie.id());
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Optional<Movie> findOne(Long id) {
        if (id == null) {
            throw new MovieDAOException("Null id provided!");
        }
        try (var movieSelectOneStatement = dbConnection.prepareStatement("""
                SELECT * FROM movies
                WHERE id = ?;
                """)) {
            movieSelectOneStatement.setLong(1, id);
            ResultSet resultSet = movieSelectOneStatement.executeQuery();
            if (resultSet.next()) {
                Object[] genres = (Object[]) resultSet.getArray("genres").getArray();
                return Optional.of(new Movie(
                        resultSet.getLong("id"),
                        resultSet.getString("original_language"),
                        resultSet.getString("overview"),
                        resultSet.getDouble("popularity"),
                        resultSet.getDate("release_date").toLocalDate(),
                        resultSet.getString("title"),
                        resultSet.getDouble("vote_average"),
                        resultSet.getLong("vote_count"),
                        Arrays.copyOf(genres, genres.length, String[].class),
                        List.of(),
                        List.of()));
            } else {
                return Optional.empty();
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<Movie> findMany() {
        // TODO implement!
        return List.of();
    }

    private void mapMovieToStatement(PreparedStatement statement, Movie movie) throws SQLException {
        statement.setString(1, movie.originalLanguage());
        statement.setString(2, movie.overview());
        statement.setDouble(3, movie.popularity());
        statement.setDate(4, Date.valueOf(movie.releaseDate()));
        statement.setString(5, movie.title());
        statement.setDouble(6, movie.voteAverage());
        statement.setDouble(7, movie.voteCount());

        Array genresArray = dbConnection.createArrayOf("VARCHAR", movie.genres());
        statement.setArray(8, genresArray);
    }
}
