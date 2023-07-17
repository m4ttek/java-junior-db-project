import java.time.LocalDate;
import java.util.List;
import java.util.Objects;

public record Movie(Long id,
                    String originalLanguage,
                    String overview,
                    Double popularity,
                    LocalDate releaseDate,
                    String title,
                    Double voteAverage,
                    Long voteCount,
                    String[] genres,
                    List<Actor> actors,
                    List<Keyword> keywords) {

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Movie otherMovie) {
            return Objects.equals(id, otherMovie.id());
        }
        return false;
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }
}
