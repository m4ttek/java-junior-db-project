import java.util.List;
import java.util.Optional;

public interface MoviesDAO {

    void save(Movie movie);

    void update(Movie movie);

    void delete(Movie movie);

    Optional<Movie> findOne(Long id);

    List<Movie> findMany();

    class MovieDAOException extends RuntimeException {

        MovieDAOException(String cause) {
            super(cause);
        }
    }
}
