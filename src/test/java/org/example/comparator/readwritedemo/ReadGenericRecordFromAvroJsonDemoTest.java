package org.example.comparator.readwritedemo;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;

class ReadGenericRecordFromAvroJsonDemoTest {

    static final String SCHEMA_FILE = "src/test/resources/user.avsc";
    static final String USERS_AVRO_JSON = "src/test/resources/users.avro.json";

    @Disabled
    @Test
    void readGenericRecordFromAvroJsonDemo() throws IOException {
        Schema schema = new Schema.Parser().parse(new File(SCHEMA_FILE));
        try (
                InputStream jsonIn = Files.newInputStream(Paths.get(USERS_AVRO_JSON))
        ) {
            GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
            JsonDecoder decoder = DecoderFactory.get().jsonDecoder(schema, jsonIn);

            System.out.println(reader.read(null, decoder));
        }
    }
}
