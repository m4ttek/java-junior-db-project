import java.util.Objects;

public record Keyword(Long id, String name) {

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Keyword otherKeyword) {
            return Objects.equals(id, otherKeyword.id());
        }
        return false;
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }
}
