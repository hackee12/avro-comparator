package org.example.comparator.comparators.naive;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.example.comparator.comparators.AvroComparator;
import org.example.comparator.comparators.AvroKeyResolver;
import org.example.comparator.domain.DeepDiff;
import org.example.comparator.domain.DiffEngineConfig;
import org.example.comparator.domain.DiffLayout;
import org.example.comparator.domain.ThinAvroDiff;
import org.example.comparator.exception.AvroComparatorException;
import org.example.comparator.util.SchemaUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static java.lang.String.format;
import static java.util.Objects.isNull;

@NoArgsConstructor
@AllArgsConstructor
@Setter
@Getter
public class NaiveComparator implements AvroComparator<DeepDiff> {

    private static final DiffLayout LAYOUT = NaiveLayout.INSTANCE;
    private static final String TYPE_NOT_EXPECTED = "This is a bug! I did not expect %s type in here.";
    private static final String TYPE_NOT_SUPPORTED_IN_DEMO = "%s type is not supported in this demo version.";

    private AvroKeyResolver avroKeyResolver;
    private DiffEngineConfig diffEngineConfig;

    @Override
    public List<ThinAvroDiff> getDiff(Schema reader, GenericRecord left, GenericRecord right) {
        if (left == right) {
            throw new AvroComparatorException(new IllegalArgumentException(
                    "This comparator does not support comparing objects to themselves."
            ));
        }
        return traverseRecordGenericRecords(reader.getName(), reader, left, right);
    }

    private List<ThinAvroDiff> traverseRecordGenericRecords(String parent, Schema recordSchema, GenericRecord left, GenericRecord right) {
        final List<ThinAvroDiff> diff = new ArrayList<>();
        final String level = parent + LAYOUT.getDelimiter();

        for (Schema.Field field : recordSchema.getFields()) {
            final Schema fieldDatumSchema = SchemaUtil.getDatumSchema(field.schema());
            final Schema.Type fieldDatumType = fieldDatumSchema.getType();
            final Object fieldValueLeft = left == null ? null : left.get(field.name());
            final Object fieldValueRight = right == null ? null : right.get(field.name());

            if (isNull(fieldValueLeft) && isNull(fieldValueRight)) {
                continue;
            }

            switch (fieldDatumType) {

                case NULL, UNION -> throw new AvroRuntimeException(new RuntimeException(
                        format(TYPE_NOT_EXPECTED, fieldDatumType)));

                case FIXED, BYTES, ENUM, MAP, ARRAY -> throw new AvroRuntimeException(new RuntimeException(
                        format(TYPE_NOT_SUPPORTED_IN_DEMO, fieldDatumType)));

                case RECORD ->
                        diff.addAll(traverseRecordGenericRecords(level + field.name(), fieldDatumSchema, (GenericRecord) fieldValueLeft, (GenericRecord) fieldValueRight));

                case STRING, INT, LONG, FLOAT, DOUBLE, BOOLEAN ->
                        diff.addAll(traverseLeaf(level, field, fieldValueLeft, fieldValueRight));
            }
        }
        return diff;
    }

    private List<ThinAvroDiff> traverseLeaf(String parent, Schema.Field leaf, Object left, Object right) {

        if (Objects.equals(left, right)) {
            return List.of();
        }

        final String leafLevel = parent + leaf.name();
        final String leafType = SchemaUtil.getDatumSchema(leaf.schema()).getType().getName();
        return List.of(
                new ThinAvroDiff(
                        leafLevel,
                        leafType,
                        left == null ? null : left.toString(),
                        right == null ? null : right.toString())
        );
    }
}
