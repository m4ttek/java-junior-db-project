import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class MoviesApplication {

    public static void main(String[] args) throws SQLException {
        try (Connection connection = DriverManager.getConnection("jdbc:h2:mem:./")) {
            new DBInitializer(connection).initDB();
            new DataLoader(connection).loadMoviesData();
        }
    }
}
