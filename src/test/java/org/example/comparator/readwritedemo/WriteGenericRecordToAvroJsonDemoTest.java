package org.example.comparator.readwritedemo;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;

class WriteGenericRecordToAvroJsonDemoTest {

    static final String SCHEMA_FILE = "src/test/resources/user.avsc";
    static final String USERS_AVRO_JSON = "src/test/resources/users.avro.json";

    @Disabled
    @Test
    void writeGenericRecordToAvroJsonDemo() throws IOException{
        Schema schema = new Schema.Parser().parse(new File(SCHEMA_FILE));

        GenericData.Record user1 = new GenericRecordBuilder(schema)
                .set("name", "User1")
                .set("favorite_number", 42)
                .set("favorite_color", "red")
                .build();

        try (
                OutputStream jsonOut = Files.newOutputStream(Paths.get(USERS_AVRO_JSON))
        ) {
            GenericDatumWriter<Object> writer = new GenericDatumWriter<>(schema);
            JsonEncoder jsonEncoder = EncoderFactory.get().jsonEncoder(schema, jsonOut);

            writer.write(user1, jsonEncoder);
            jsonEncoder.flush();
        }
    }
}
