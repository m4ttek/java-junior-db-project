import java.util.Objects;

public record Actor(Long id, String name, String character) {

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Actor otherActor) {
            return Objects.equals(id, otherActor.id()) && Objects.equals(character, otherActor.character());
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, character);
    }
}
