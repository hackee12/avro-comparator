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

    private static final String TEMPLATE_RECORD_DELIMITER = "%s.";
    private static final String TEMPLATE_RECORD_FIELD_DELIMITER = "%s%s.";
    private static final String TEMPLATE_ARRAY_FIELD_DELIMITER = "%s%s[%d]";

    private static final String TYPE_NOT_EXPECTED = "This is a bug! I did not expect %s type in here.";
    private static final String TYPE_NOT_SUPPORTED_IN_DEMO = "%s type is not supported in this demo version.";

    private AvroKeyResolver avroKeyResolver;
    private DiffEngineConfig diffEngineConfig;

    @Override
    public List<ThinAvroDiff> getDiff(Schema recordSchema, GenericRecord left, GenericRecord right) {
        if (left == right) {
            throw new AvroComparatorException(new IllegalArgumentException(
                    "This comparator does not support comparing objects to themselves."
            ));
        }
        final String nodeLevel = String.format(TEMPLATE_RECORD_DELIMITER, recordSchema.getName());
        return traverseRecordFields(nodeLevel, recordSchema, left, right);
    }

    private List<ThinAvroDiff> traverseRecordFields(
            String nodeLevel, Schema recordSchema, GenericRecord left, GenericRecord right
    ) {
        final List<ThinAvroDiff> diff = new ArrayList<>();
        for (Schema.Field field : recordSchema.getFields()) {
            final Object fieldValueLeft = left == null ? null : left.get(field.name());
            final Object fieldValueRight = right == null ? null : right.get(field.name());
            if (isNull(fieldValueLeft) && isNull(fieldValueRight)) {
                continue;
            }
            final Schema.Type fieldDatumType = SchemaUtil.getDatumSchema(field.schema()).getType();
            switch (fieldDatumType) {
                case NULL, UNION -> throw new AvroRuntimeException(new RuntimeException(
                        format(TYPE_NOT_EXPECTED, fieldDatumType)));
                case FIXED, BYTES, ENUM, MAP -> throw new AvroRuntimeException(new RuntimeException(
                        format(TYPE_NOT_SUPPORTED_IN_DEMO, fieldDatumType)));
                case RECORD -> diff.addAll(getRecordDiff(
                        nodeLevel, field, (GenericRecord) fieldValueLeft, (GenericRecord) fieldValueRight));
                case STRING, INT, LONG, FLOAT, DOUBLE, BOOLEAN ->
                        diff.addAll(getLeafDiff(nodeLevel, field, fieldValueLeft, fieldValueRight));
                case ARRAY -> diff.addAll(getArrayDiff(
                        nodeLevel, field, (List<?>) fieldValueLeft, (List<?>) fieldValueRight));
            }
        }
        return diff;
    }

    private List<ThinAvroDiff> getRecordDiff(
            String parent, Schema.Field recordField, GenericRecord left, GenericRecord right
    ) {
        final String nodeLevel = String.format(TEMPLATE_RECORD_FIELD_DELIMITER, parent, recordField.name());
        final Schema recordSchema = SchemaUtil.getDatumSchema(recordField.schema());
        return traverseRecordFields(nodeLevel, recordSchema, left, right);
    }

    private List<ThinAvroDiff> getLeafDiff(String parent, Schema.Field leaf, Object left, Object right) {
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
                        right == null ? null : right.toString()
                )
        );
    }

    private List<ThinAvroDiff> getArrayDiff(String parent, Schema.Field field, List<?> left, List<?> right) {
        final int leftSize = isNull(left) ? 0 : left.size();
        final int rightSize = isNull(right) ? 0 : right.size();

        var elementSchemaType =
                SchemaUtil.getDatumSchema(field.schema())
                        .getElementType()
                        .getType();
        switch (elementSchemaType) {
            case STRING, INT, LONG, FLOAT, DOUBLE, BOOLEAN -> {
                final List<ThinAvroDiff> diffs = new ArrayList<>();
                for (int i = 0; i < Math.max(leftSize, rightSize); i++) {
                    var itemLeft = i < leftSize ? left.get(i) : null;
                    var itemRight = i < rightSize ? right.get(i) : null;
                    if (!Objects.equals(itemLeft, itemRight)) {
                        diffs.add(
                                new ThinAvroDiff(
                                        String.format(TEMPLATE_ARRAY_FIELD_DELIMITER, parent, field.name(), i),
                                        elementSchemaType.getName(),
                                        itemLeft == null ? null : itemLeft.toString(),
                                        itemRight == null ? null : itemRight.toString()
                                )
                        );
                    }
                }
                return diffs;
            }
            case FIXED, BYTES, ENUM, MAP, ARRAY, RECORD -> throw new AvroRuntimeException(new RuntimeException(
                    format(TYPE_NOT_SUPPORTED_IN_DEMO, elementSchemaType)));
            default -> throw new AvroRuntimeException(new RuntimeException(
                    format(TYPE_NOT_EXPECTED, elementSchemaType)));
        }
    }
}
