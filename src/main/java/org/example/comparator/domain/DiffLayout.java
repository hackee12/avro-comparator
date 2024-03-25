package org.example.comparator.domain;

public interface DiffLayout {
    default String getDelimiter() {
        return ".";
    }
}
