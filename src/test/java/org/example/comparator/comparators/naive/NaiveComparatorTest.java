package org.example.comparator.comparators.naive;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.example.comparator.domain.DiffEngineConfig;
import org.example.comparator.domain.ThinAvroDiff;
import org.example.comparator.util.SchemaUtil;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class NaiveComparatorTest {
    private static final DiffEngineConfig DUMMY_CONFIG = new DiffEngineConfig() {
    };
    private static final NaiveComparator comparator =
            new NaiveComparator(record -> record.get("id").toString(), DUMMY_CONFIG);
    private static final String SCHEMA_FILE = "src/test/resources/user.avsc";
    private static Schema SCHEMA;

    @BeforeAll
    static void beforeAll() throws IOException {
        SCHEMA = new Schema.Parser().parse(new File(SCHEMA_FILE));
    }

    @Test
    void compareNullsThrowsException() {
        assertThrows(RuntimeException.class, () -> comparator.getDiff(SCHEMA, null, null));
    }

    @Test
    void compareNonNullToNullReturnsNonNullDiff() {
        var record = new GenericRecordBuilder(SCHEMA)
                .set("id", 123)
                .set("name", "ABC")
                .build();

        assertNotNull(comparator.getDiff(SCHEMA, record, null),
                "Compare non-null Left to null Right returned null Diff");
        assertNotNull(comparator.getDiff(SCHEMA, null, record),
                "Compare null Left to non-null Right returned null Diff");
    }

    @Test
    void compareNonNullLeftToNullRightReturnsBasicDiff() {
        final int id = 42;
        final GenericRecord leftRecord = new GenericRecordBuilder(SCHEMA)
                .set("id", id)
                .set("name", "ABC")
                .build();
        var expectedDiff = List.of(
                new ThinAvroDiff("User.id", "string", "42", null),
                new ThinAvroDiff("User.name", "string", "ABC", null)
        );
        assertEquals(expectedDiff, comparator.getDiff(SCHEMA, leftRecord, null));
    }

    @Test
    void compareNullLeftToNonNullRightReturnsBasicDiff() throws IOException {
        final int id = 43;
        final GenericRecord rightRecord =
                new GenericRecordBuilder(new Schema.Parser().parse(new File(SCHEMA_FILE)))
                        .set("id", id)
                        .set("name", "ABCd")
                        .build();
        var expectedDiff = List.of(
                new ThinAvroDiff("User.id", "string", null, "43"),
                new ThinAvroDiff("User.name", "string", null, "ABCd")
        );
        assertEquals(expectedDiff, comparator.getDiff(SCHEMA, null, rightRecord));
    }

    @Test
    void emptyDiff() {
        final GenericData.Record addressLeft =
                new GenericRecordBuilder(
                        SchemaUtil.getDatumSchema(SCHEMA.getField("address").schema())
                ).set("line1", "Address Line 1").build();
        final GenericData.Record addressRight =
                new GenericRecordBuilder(
                        SchemaUtil.getDatumSchema(SCHEMA.getField("address").schema())
                ).set("line1", "Address Line 1").build();
        final GenericRecord left =
                new GenericRecordBuilder(SCHEMA)
                        .set("id", 42)
                        .set("name", "ABC")
                        .set("favorite_color", "red")
                        .set("address", addressLeft)
                        .build();
        final GenericRecord right =
                new GenericRecordBuilder(SCHEMA)
                        .set("id", 42)
                        .set("name", "ABC")
                        .set("favorite_color", "red")
                        .set("address", addressRight)
                        .build();
        assertEquals(List.of(), comparator.getDiff(SCHEMA, left, right));
    }

    @Test
    void levelStringDiff() {
        final GenericRecord left =
                new GenericRecordBuilder(SCHEMA)
                        .set("id", 42)
                        .set("name", "ABC")
                        .set("favorite_color", "red")
                        .build();
        final GenericRecord right =
                new GenericRecordBuilder(SCHEMA)
                        .set("id", 42)
                        .set("name", "abc")
                        .set("favorite_color", "red")
                        .build();
        final List<Object> expectedDiff = List.of(
                new ThinAvroDiff("User.name", "string", "ABC", "abc")
        );
        assertEquals(expectedDiff, comparator.getDiff(SCHEMA, left, right));
    }

    @Test
    void levelIntDiff() {
        final GenericRecord left =
                new GenericRecordBuilder(SCHEMA)
                        .set("id", 42)
                        .set("name", "ABC")
                        .set("favorite_number", 11)
                        .build();
        final GenericRecord right =
                new GenericRecordBuilder(SCHEMA)
                        .set("id", 42)
                        .set("name", "ABC")
                        .set("favorite_number", 22)
                        .build();
        final List<Object> expectedDiff = List.of(
                new ThinAvroDiff("User.favorite_number", "int", "11", "22")
        );
        assertEquals(expectedDiff, comparator.getDiff(SCHEMA, left, right));
    }

    @Test
    void levelPrimitivesDiff() {
        final GenericRecord left =
                new GenericRecordBuilder(SCHEMA)
                        .set("id", 42)
                        .set("name", "ABC")
                        .set("favorite_number", 11)
                        .build();
        final GenericRecord right =
                new GenericRecordBuilder(SCHEMA)
                        .set("id", 42)
                        .set("name", "abc")
                        .set("favorite_number", 22)
                        .build();
        final List<Object> expectedDiff = List.of(
                new ThinAvroDiff("User.name", "string", "ABC", "abc"),
                new ThinAvroDiff("User.favorite_number", "int", "11", "22")
        );
        assertEquals(expectedDiff, comparator.getDiff(SCHEMA, left, right));
    }

    @Test
    void missingNestedRecord() {
        final GenericData.Record addressLeft =
                new GenericRecordBuilder(
                        SchemaUtil.getDatumSchema(SCHEMA.getField("address").schema())
                ).set("line1", "Address Line 1").build();
        final GenericRecord left =
                new GenericRecordBuilder(SCHEMA)
                        .set("id", 42)
                        .set("name", "ABC")
                        .set("favorite_number", 11)
                        .set("address", addressLeft)
                        .build();
        final GenericRecord right =
                new GenericRecordBuilder(SCHEMA)
                        .set("id", 42)
                        .set("name", "abc")
                        .set("favorite_number", 22)
                        .build();
        final List<Object> expectedDiff = List.of(
                new ThinAvroDiff("User.name", "string", "ABC", "abc"),
                new ThinAvroDiff("User.favorite_number", "int", "11", "22"),
                new ThinAvroDiff("User.address.line1", "string", "Address Line 1", null)
        );
        assertEquals(expectedDiff, comparator.getDiff(SCHEMA, left, right));
    }

    @Test
    void missingNestedString() {
        final GenericData.Record addressLeft =
                new GenericRecordBuilder(
                        SchemaUtil.getDatumSchema(SCHEMA.getField("address").schema())
                ).set("line1", "Address Line 1").set("line2", "Address Line 2").build();
        final GenericData.Record addressRight =
                new GenericRecordBuilder(
                        SchemaUtil.getDatumSchema(SCHEMA.getField("address").schema())
                ).set("line1", "Address Line 1").build();
        final GenericRecord left =
                new GenericRecordBuilder(SCHEMA)
                        .set("id", 42)
                        .set("name", "ABC")
                        .set("favorite_number", 11)
                        .set("address", addressLeft)
                        .build();
        final GenericRecord right =
                new GenericRecordBuilder(SCHEMA)
                        .set("id", 42)
                        .set("name", "abc")
                        .set("favorite_number", 22)
                        .set("address", addressRight)
                        .build();
        final List<Object> expectedDiff = List.of(
                new ThinAvroDiff("User.name", "string", "ABC", "abc"),
                new ThinAvroDiff("User.favorite_number", "int", "11", "22"),
                new ThinAvroDiff("User.address.line2", "string", "Address Line 2", null)
        );
        assertEquals(expectedDiff, comparator.getDiff(SCHEMA, left, right));
    }
}