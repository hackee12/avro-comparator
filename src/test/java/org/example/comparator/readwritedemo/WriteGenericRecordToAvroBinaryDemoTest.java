package org.example.comparator.readwritedemo;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

class WriteGenericRecordToAvroBinaryDemoTest {
    static final String SCHEMA_FILE = "src/test/resources/user.avsc";
    static final String AVRO_BINARY = "src/test/resources/users.avro";

    @Disabled
    @Test
    void flushGenericRecordToAvroBinary() throws IOException {
        Schema schema = new Schema.Parser().parse(new File(SCHEMA_FILE));

        GenericData.Record user1 = new GenericRecordBuilder(schema)
                .set("name", "User1")
                .set("favorite_number", 42)
                .set("favorite_color", "red")
                .build();

        DataFileWriter<GenericRecord> dataFileWriter =
                new DataFileWriter<>(new GenericDatumWriter<GenericRecord>(schema));
        dataFileWriter.create(schema, new File(AVRO_BINARY));
        dataFileWriter.append(user1);
        dataFileWriter.close();

    }
}
