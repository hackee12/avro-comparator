package org.example.comparator.util;

import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SchemaUtilTest {
    private static final String SCHEMA_FILE = "src/test/resources/user.avsc";

    @Test
    void getDataType() throws IOException {
        Schema schema = new Schema.Parser().parse(new File(SCHEMA_FILE));
        Schema idSchema = schema.getField("id").schema();
        Schema numberSchema = schema.getField("favorite_number").schema();
        Schema colorSchema = schema.getField("favorite_color").schema();

        assertEquals(
                Schema.Type.STRING,
                SchemaUtil.getDatumSchema(idSchema).getType()
        );
        assertEquals(
                Schema.Type.INT,
                SchemaUtil.getDatumSchema(numberSchema).getType()
        );
        assertEquals(
                Schema.Type.STRING,
                SchemaUtil.getDatumSchema(colorSchema).getType()
        );
    }
}