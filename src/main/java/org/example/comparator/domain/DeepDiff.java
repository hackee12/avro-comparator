package org.example.comparator.domain;

public interface DeepDiff {
    String getElementPath();
    String getElementType();
    String getValueLeft();
    String getValueRight();
}
