package org.example.comparator.util;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.avro.Schema;

import static org.apache.avro.Schema.Type.NULL;
import static org.apache.avro.Schema.Type.UNION;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class SchemaUtil {
    public static Schema getDatumSchema(Schema nullableType) {
        final Schema.Type type = nullableType.getType();
        if (UNION.equals(type)) {
            return nullableType.getTypes().stream()
                    .filter(innerType -> !NULL.equals(innerType.getType()))
                    .findFirst()
                    .orElseThrow();
        }
        return nullableType;
    }
}
