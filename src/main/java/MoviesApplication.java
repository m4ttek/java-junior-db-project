import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Optional;

public class MoviesApplication {

    public static void main(String[] args) throws SQLException {
        try (Connection connection = DriverManager.getConnection("jdbc:h2:mem:./")) {
            new DBInitializer(connection).initDB();
            new DataLoader(connection).loadMoviesData();
            new DataLoader(connection).loadKeywordsData();
            new DataLoader(connection).loadActorsData();

            MoviesDAO moviesDAO = new DatabaseMovieDAO(connection);
            Optional<Movie> someMovie = moviesDAO.findOne(385687L);
            moviesDAO.delete(someMovie.get());
        }
    }
}
