import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.stream.Collectors;

public class DBInitializer {

    private static String[] filePaths = {
            "/init/movies.sql",
            "/init/actors.sql",
            "/init/keywords.sql",
            "/init/movies_actors.sql",
            "/init/movies_keywords.sql"
    };

    private final Connection dbConnection;

    public DBInitializer(Connection dbConnection) {
        this.dbConnection = dbConnection;
    }

    void initDB() {
        try (var stat = dbConnection.createStatement()) {
            for (var path: filePaths) {
                loadCreateTableAndExecute(path, stat);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void loadCreateTableAndExecute(String resourcePath, Statement statement) throws SQLException {
        try (var createTableReader = new BufferedReader(new InputStreamReader(this.getClass().getResourceAsStream(resourcePath)))) {
            String sqlContent = createTableReader.lines().collect(Collectors.joining("\n"));
            statement.execute(sqlContent);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
