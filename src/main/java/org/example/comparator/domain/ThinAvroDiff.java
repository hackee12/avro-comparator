package org.example.comparator.domain;

public record ThinAvroDiff(String path, String type, String left, String right) implements DeepDiff {

    @Override
    public String getElementPath() {
        return path();
    }

    @Override
    public String getElementType() {
        return type();
    }

    @Override
    public String getValueLeft() {
        return left();
    }

    @Override
    public String getValueRight() {
        return right();
    }
}
