package org.example.comparator.readwritedemo;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.io.JsonEncoder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;

class AvroJsonDemoTest {

    static final String SCHEMA_FILE = "src/test/resources/user.avsc";

    @Disabled
    @Test
    void writeToThenReadFromAvroJson() throws IOException {
        Schema schema = new Schema.Parser().parse(new File(SCHEMA_FILE));

        GenericData.Record userWrite = new GenericRecordBuilder(schema)
                .set("id", "123")
                .set("name", "User1")
                .set("favorite_number", 42)
                .set("favorite_color", "red")
                .build();

        try (var byteOut = new ByteArrayOutputStream()) {
            GenericDatumWriter<Object> writer = new GenericDatumWriter<>(schema);
            JsonEncoder jsonEncoder = EncoderFactory.get().jsonEncoder(schema, byteOut);

            writer.write(userWrite, jsonEncoder);
            jsonEncoder.flush();

            try (var byteIn = new ByteArrayInputStream(byteOut.toByteArray())) {
                GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
                JsonDecoder decoder = DecoderFactory.get().jsonDecoder(schema, byteIn);

                GenericRecord userRead = reader.read(null, decoder);
                Assertions.assertEquals(userWrite, userRead);
            }
        }
    }
}
