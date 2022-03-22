package net.christophschubert.cp.testcontainers.util;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class Tag implements Comparable<Tag> {
    private final List<Integer> parts;

    public Tag(List<Integer> parts) {
        this.parts = parts;
    }

    public Tag(String tag) {
        this.parts = Arrays.stream(tag.split(Pattern.quote("."))).map(Integer::parseInt).collect(Collectors.toList());
    }

    @Override
    public int compareTo(Tag o) {
        final var thisIterator = this.parts.iterator();
        final var otherIterator = o.parts.iterator();
        while (thisIterator.hasNext() || otherIterator.hasNext()) {
            final var thisValue = thisIterator.hasNext() ? thisIterator.next() : Integer.valueOf(0);
            final var otherValue = otherIterator.hasNext() ? otherIterator.next() : Integer.valueOf(0);
            final int cmp = thisValue.compareTo(otherValue);
            if (cmp != 0) return cmp;
        }
        return 0;
    }

    public boolean atLeast(Tag minVersion) {
        return this.compareTo(minVersion) >= 0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Tag tag = (Tag) o;
        return Objects.equals(parts, tag.parts);
    }

    @Override
    public int hashCode() {
        return Objects.hash(parts);
    }


    @Override
    public String toString() {
        return "Tag{" +
                "parts=" + parts +
                '}';
    }

}
