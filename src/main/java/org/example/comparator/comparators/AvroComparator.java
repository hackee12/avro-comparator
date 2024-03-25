package org.example.comparator.comparators;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import java.util.List;

public interface AvroComparator<T> {
    List<? extends T> getDiff(Schema reader, GenericRecord left, GenericRecord right);
}
