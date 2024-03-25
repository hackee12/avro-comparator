package org.example.comparator.exception;

public class AvroComparatorException extends RuntimeException {
    public AvroComparatorException(String message) {
        super(message);
    }
    public AvroComparatorException(String message, Throwable t) {
        super(message, t);
    }
    public AvroComparatorException(Throwable t) {
        super(t);
    }
}
