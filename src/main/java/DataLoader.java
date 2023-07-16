import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

public class DataLoader {

    private final Connection dbConnection;

    private final String[] MOVIES_HEADERS = { "genres_ids", "id", "original_language", "overview", "popularity", "release_date", "title", "vote_average", "vote_count", "genres"};

    public DataLoader(Connection dbConnection) {
        this.dbConnection = dbConnection;
    }

    void loadMoviesData() {
        try (var createTableReader = new BufferedReader(new InputStreamReader(this.getClass().getResourceAsStream("/data/movies_dataset.csv")));
             var statement = dbConnection.createStatement()) {
            CSVFormat csvFormat = CSVFormat.DEFAULT.builder()
                    .setHeader(MOVIES_HEADERS)
                    .setSkipHeaderRecord(true)
                    .build();
            Iterable<CSVRecord> records = csvFormat.parse(createTableReader);
            Set<String> setForDuplicates = new HashSet<>();
            for (CSVRecord record : records) {
                if (setForDuplicates.contains(record.get("id"))) {
                    continue;
                }
                setForDuplicates.add(record.get("id"));
                statement.addBatch("INSERT INTO movies VALUES (%s, '%s', '%s', %s, %s, '%s', %s, %s, ARRAY[ %s ]);".formatted(
                        record.get("id"),
                        record.get("original_language"),
                        record.get("overview").replaceAll("'", "''"),
                        record.get("popularity"),
                        record.get("release_date").isBlank() ? "NULL" : String.join("","'", record.get("release_date"), "'"),
                        record.get("title").replaceAll("'", "''"),
                        record.get("vote_average"),
                        record.get("vote_count"),
                        Arrays.stream(record.get("genres").split(", ")).collect(Collectors.joining("', '", "'", "'"))));
            }
            statement.executeBatch();
        } catch (IOException | SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
