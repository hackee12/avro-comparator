package org.example.comparator.comparators;

import org.apache.avro.generic.GenericRecord;

public interface AvroKeyResolver {
    String getKey(GenericRecord record);
}
