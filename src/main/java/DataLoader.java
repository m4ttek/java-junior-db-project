import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

public class DataLoader {

    private static ObjectMapper objectMapper = new ObjectMapper();
    static {
        objectMapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
        objectMapper.configure(JsonParser.Feature.ALLOW_BACKSLASH_ESCAPING_ANY_CHARACTER, true);
    }

    private final Connection dbConnection;

    private final String[] MOVIES_HEADERS = { "genres_ids", "id", "original_language", "overview", "popularity", "release_date", "title", "vote_average", "vote_count", "genres"};
    private final String[] KEYWORDS_HEADERS = { "lp", "id", "keywords"};
    private final String[] ACTORS_HEADERS = { "id", "cast"};

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

    public void loadKeywordsData() {
        try (var createTableReader = new BufferedReader(new InputStreamReader(this.getClass().getResourceAsStream("/data/keywords_dataframe.csv")));
             var statement = dbConnection.createStatement()) {
            CSVFormat csvFormat = CSVFormat.DEFAULT.builder()
                    .setHeader(KEYWORDS_HEADERS)
                    .setSkipHeaderRecord(true)
                    .build();
            Iterable<CSVRecord> records = csvFormat.parse(createTableReader);
            Set<String> setForDuplicates = new HashSet<>();

            Map<Integer, List<Integer>> movieToKeywordsIds = new HashMap<>();
            Map<Integer, String> keywordsMap = new HashMap<>();

            for (CSVRecord record : records) {
                String movieId = record.get("id");
                if (setForDuplicates.contains(record.get("id"))) {
                    continue;
                }
                setForDuplicates.add(movieId);

                List<Map<String, Object>> keywords = objectMapper.readValue(record.get("keywords"), List.class);

                List<Integer> movieKeywordIds = new ArrayList<>();
                for (var keywordObject: keywords) {
                    Integer keywordId = (Integer) keywordObject.get("id");
                    String keywordName = (String) keywordObject.get("name");
                    keywordsMap.put(keywordId, keywordName);
                    movieKeywordIds.add(keywordId);
                }
                movieToKeywordsIds.put(Integer.valueOf(movieId), movieKeywordIds);
            }

            for (var keyword: keywordsMap.entrySet()) {
                statement.addBatch("INSERT INTO keywords VALUES ( %d, '%s' )".formatted(keyword.getKey(), keyword.getValue().replaceAll("'", "''")));
            }

            for (var movieToKeywords: movieToKeywordsIds.entrySet()) {
                for (var keywordId: movieToKeywords.getValue()) {
                    statement.addBatch("INSERT INTO movies_keywords VALUES ( %d, %d )".formatted(movieToKeywords.getKey(), keywordId));
                }
            }

            statement.executeBatch();
        } catch (IOException | SQLException e) {
            throw new RuntimeException(e);
        }

    }

    public void loadActorsData() {

        try (var createTableReader = new BufferedReader(new InputStreamReader(this.getClass().getResourceAsStream("/data/cast_dataset.csv")));
             var statement = dbConnection.createStatement()) {
            CSVFormat csvFormat = CSVFormat.DEFAULT.builder()
                    .setHeader(ACTORS_HEADERS)
                    .setSkipHeaderRecord(true)
                    .build();
            Iterable<CSVRecord> records = csvFormat.parse(createTableReader);
            Set<String> setForDuplicates = new HashSet<>();

            Map<Integer, Map<Integer, String>> movieToActorsIdsWithCharacter = new HashMap<>();
            Map<Integer, String> actorsMap = new HashMap<>();

            for (CSVRecord record : records) {
                String movieId = record.get("id");
                if (setForDuplicates.contains(record.get("id"))) {
                    continue;
                }
                setForDuplicates.add(movieId);

                List<Map<String, Object>> actors = objectMapper.readValue(record.get("cast"), List.class);

                Map<Integer, String> movieActorIdAndCharacter = new HashMap<>();
                for (var actorObject: actors) {
                    Integer actorId = (Integer) actorObject.get("cast_id");
                    String actorName = (String) actorObject.get("name");
                    String actorCharacter = (String) actorObject.get("character");
                    actorsMap.put(actorId, actorName);

                    movieActorIdAndCharacter.put(actorId, actorCharacter);
                }
                movieToActorsIdsWithCharacter.put(Integer.valueOf(movieId), movieActorIdAndCharacter);
            }

            actorsMap.remove(null);
            for (var actor: actorsMap.entrySet()) {
                statement.addBatch("INSERT INTO actors VALUES ( %d, '%s' )".formatted(actor.getKey(), actor.getValue().replaceAll("'", "''")));
            }

            for (var movieToActor: movieToActorsIdsWithCharacter.entrySet()) {
                for (var actorIdWithCharacter: movieToActor.getValue().entrySet()) {
                    statement.addBatch("INSERT INTO movies_actors VALUES ( %d, %d, '%s' )".formatted(movieToActor.getKey(), actorIdWithCharacter.getKey(), actorIdWithCharacter.getValue().replaceAll("'", "''")));
                }
            }

            statement.executeBatch();
        } catch (IOException | SQLException e) {
            throw new RuntimeException(e);
        }

    }
}
