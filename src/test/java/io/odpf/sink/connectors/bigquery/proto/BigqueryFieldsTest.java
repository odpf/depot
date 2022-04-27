package io.odpf.sink.connectors.bigquery.proto;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.protobuf.DescriptorProtos;
import io.odpf.sink.connectors.message.proto.Constants;
import io.odpf.sink.connectors.message.proto.ProtoField;
import io.odpf.sink.connectors.message.proto.TestProtoUtil;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;

public class BigqueryFieldsTest {

    private final Map<DescriptorProtos.FieldDescriptorProto.Type, LegacySQLTypeName> expectedType = new HashMap<DescriptorProtos.FieldDescriptorProto.Type, LegacySQLTypeName>() {{
        put(DescriptorProtos.FieldDescriptorProto.Type.TYPE_BYTES, LegacySQLTypeName.BYTES);
        put(DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING, LegacySQLTypeName.STRING);
        put(DescriptorProtos.FieldDescriptorProto.Type.TYPE_ENUM, LegacySQLTypeName.STRING);
        put(DescriptorProtos.FieldDescriptorProto.Type.TYPE_BOOL, LegacySQLTypeName.BOOLEAN);
        put(DescriptorProtos.FieldDescriptorProto.Type.TYPE_DOUBLE, LegacySQLTypeName.FLOAT);
        put(DescriptorProtos.FieldDescriptorProto.Type.TYPE_FLOAT, LegacySQLTypeName.FLOAT);
    }};

    @Test
    public void shouldTestConvertToSchemaSuccessful() {
        List<ProtoField> nestedBQFields = new ArrayList<>();
        nestedBQFields.add(TestProtoUtil.createProtoField("field0_bytes", DescriptorProtos.FieldDescriptorProto.Type.TYPE_BYTES, DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL));
        nestedBQFields.add(TestProtoUtil.createProtoField("field1_string", DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING, DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL));
        nestedBQFields.add(TestProtoUtil.createProtoField("field2_bool", DescriptorProtos.FieldDescriptorProto.Type.TYPE_BOOL, DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL));
        nestedBQFields.add(TestProtoUtil.createProtoField("field3_enum", DescriptorProtos.FieldDescriptorProto.Type.TYPE_ENUM, DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL));
        nestedBQFields.add(TestProtoUtil.createProtoField("field4_double", DescriptorProtos.FieldDescriptorProto.Type.TYPE_DOUBLE, DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL));
        nestedBQFields.add(TestProtoUtil.createProtoField("field5_float", DescriptorProtos.FieldDescriptorProto.Type.TYPE_FLOAT, DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL));


        List<Field> fields = BigqueryFields.generateBigquerySchema(TestProtoUtil.createProtoField(nestedBQFields));
        assertEquals(nestedBQFields.size(), fields.size());
        IntStream.range(0, nestedBQFields.size())
                .forEach(index -> {
                    assertEquals(Field.Mode.NULLABLE, fields.get(index).getMode());
                    assertEquals(nestedBQFields.get(index).getName(), fields.get(index).getName());
                    assertEquals(expectedType.get(nestedBQFields.get(index).getType()), fields.get(index).getType());
                });
    }

    @Test
    public void shouldTestShouldConvertIntegerDataTypes() {
        List<DescriptorProtos.FieldDescriptorProto.Type> allIntTypes = new ArrayList<DescriptorProtos.FieldDescriptorProto.Type>() {{
            add(DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64);
            add(DescriptorProtos.FieldDescriptorProto.Type.TYPE_UINT64);
            add(DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32);
            add(DescriptorProtos.FieldDescriptorProto.Type.TYPE_UINT32);
            add(DescriptorProtos.FieldDescriptorProto.Type.TYPE_FIXED64);
            add(DescriptorProtos.FieldDescriptorProto.Type.TYPE_FIXED32);
            add(DescriptorProtos.FieldDescriptorProto.Type.TYPE_SFIXED32);
            add(DescriptorProtos.FieldDescriptorProto.Type.TYPE_SFIXED64);
            add(DescriptorProtos.FieldDescriptorProto.Type.TYPE_SINT32);
            add(DescriptorProtos.FieldDescriptorProto.Type.TYPE_SINT64);
        }};

        List<ProtoField> nestedBQFields = IntStream.range(0, allIntTypes.size())
                .mapToObj(index -> TestProtoUtil.createProtoField("field-" + index, allIntTypes.get(index), DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL))
                .collect(Collectors.toList());


        List<Field> fields = BigqueryFields.generateBigquerySchema(TestProtoUtil.createProtoField(nestedBQFields));
        assertEquals(nestedBQFields.size(), fields.size());
        IntStream.range(0, nestedBQFields.size())
                .forEach(index -> {
                    assertEquals(Field.Mode.NULLABLE, fields.get(index).getMode());
                    assertEquals(nestedBQFields.get(index).getName(), fields.get(index).getName());
                    assertEquals(LegacySQLTypeName.INTEGER, fields.get(index).getType());
                });
    }


    @Test
    public void shouldTestShouldConvertNestedField() {
        List<ProtoField> nestedBQFields = new ArrayList<>();
        nestedBQFields.add(TestProtoUtil.createProtoField("field1_level2_nested", DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING, DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL));
        nestedBQFields.add(TestProtoUtil.createProtoField("field2_level2_nested", DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING, DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL));

        ProtoField protoField = TestProtoUtil.createProtoField(new ArrayList<ProtoField>() {{
            add(TestProtoUtil.createProtoField("field1_level1",
                    DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING,
                    DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL));
            add(TestProtoUtil.createProtoField("field2_level1_message",
                    "some.type.name",
                    DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE,
                    DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL,
                    nestedBQFields));
        }});


        List<Field> fields = BigqueryFields.generateBigquerySchema(protoField);

        assertEquals(protoField.getFields().size(), fields.size());
        assertEquals(nestedBQFields.size(), fields.get(1).getSubFields().size());

        assertBqField(protoField.getFields().get(0).getName(), LegacySQLTypeName.STRING, Field.Mode.NULLABLE, fields.get(0));
        assertBqField(protoField.getFields().get(1).getName(), LegacySQLTypeName.RECORD, Field.Mode.NULLABLE, fields.get(1));
        assertBqField(nestedBQFields.get(0).getName(), LegacySQLTypeName.STRING, Field.Mode.NULLABLE, fields.get(1).getSubFields().get(0));
        assertBqField(nestedBQFields.get(1).getName(), LegacySQLTypeName.STRING, Field.Mode.NULLABLE, fields.get(1).getSubFields().get(1));

    }


    @Test
    public void shouldTestShouldConvertMultiNestedFields() {
        List<ProtoField> nestedBQFields = new ArrayList<ProtoField>() {{
            add(TestProtoUtil.createProtoField("field1_level3_nested",
                    DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING,
                    DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL));
            add(TestProtoUtil.createProtoField("field2_level3_nested",
                    DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING,
                    DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL));
        }};

        ProtoField protoField = TestProtoUtil.createProtoField(new ArrayList<ProtoField>() {{
            add(TestProtoUtil.createProtoField("field1_level1",
                    DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING,
                    DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL));

            add(TestProtoUtil.createProtoField(
                    "field2_level1_message",
                    "some.type.name",
                    DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE,
                    DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL,
                    new ArrayList<ProtoField>() {{
                        add(TestProtoUtil.createProtoField(
                                "field1_level2",
                                DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING,
                                DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL));
                        add(TestProtoUtil.createProtoField(
                                "field2_level2_message",
                                "some.type.name",
                                DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE,
                                DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL,
                                nestedBQFields));
                        add(TestProtoUtil.createProtoField(
                                "field3_level2",
                                DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING,
                                DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL));
                        add(TestProtoUtil.createProtoField(
                                "field4_level2_message",
                                "some.type.name",
                                DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE,
                                DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL,
                                nestedBQFields));
                    }}
            ));
        }});

        List<Field> fields = BigqueryFields.generateBigquerySchema(protoField);


        assertEquals(protoField.getFields().size(), fields.size());
        assertEquals(4, fields.get(1).getSubFields().size());
        assertEquals(2, fields.get(1).getSubFields().get(1).getSubFields().size());
        assertEquals(2, fields.get(1).getSubFields().get(3).getSubFields().size());
        assertMultipleFields(nestedBQFields, fields.get(1).getSubFields().get(1).getSubFields());
        assertMultipleFields(nestedBQFields, fields.get(1).getSubFields().get(3).getSubFields());
    }

    @Test
    public void shouldTestConvertToSchemaForTimestamp() {
        ProtoField protoField = TestProtoUtil.createProtoField(new ArrayList<ProtoField>() {{
            add(TestProtoUtil.createProtoField("field1_timestamp",
                    Constants.ProtobufTypeName.TIMESTAMP_PROTOBUF_TYPE_NAME,
                    DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE,
                    DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL));
        }});

        List<Field> fields = BigqueryFields.generateBigquerySchema(protoField);

        assertEquals(protoField.getFields().size(), fields.size());
        assertBqField(protoField.getFields().get(0).getName(), LegacySQLTypeName.TIMESTAMP, Field.Mode.NULLABLE, fields.get(0));
    }


    @Test
    public void shouldTestConvertToSchemaForSpecialFields() {
        ProtoField protoField = TestProtoUtil.createProtoField(new ArrayList<ProtoField>() {{
            add(TestProtoUtil.createProtoField("field1_struct",
                    Constants.ProtobufTypeName.STRUCT_PROTOBUF_TYPE_NAME,
                    DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE,
                    DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL));
            add(TestProtoUtil.createProtoField("field2_bytes",
                    DescriptorProtos.FieldDescriptorProto.Type.TYPE_BYTES,
                    DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL));

            add(TestProtoUtil.createProtoField("field3_duration",
                    "." + com.google.protobuf.Duration.getDescriptor().getFullName(),
                    DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE,
                    DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL,
                    new ArrayList<ProtoField>() {
                        {
                            add(TestProtoUtil.createProtoField("duration_seconds",
                                    DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64,
                                    DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL));

                            add(TestProtoUtil.createProtoField("duration_nanos",
                                    DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32,
                                    DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL));

                        }
                    }));

            add(TestProtoUtil.createProtoField("field3_date",
                    "." + com.google.type.Date.getDescriptor().getFullName(),
                    DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE,
                    DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL,
                    new ArrayList<ProtoField>() {
                        {
                            add(TestProtoUtil.createProtoField("year",
                                    DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64,
                                    DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL));

                            add(TestProtoUtil.createProtoField("month",
                                    DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32,
                                    DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL));

                            add(TestProtoUtil.createProtoField("day",
                                    DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32,
                                    DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL));

                        }
                    }));

        }});

        List<Field> fields = BigqueryFields.generateBigquerySchema(protoField);

        assertEquals(protoField.getFields().size(), fields.size());
        assertBqField(protoField.getFields().get(0).getName(), LegacySQLTypeName.STRING, Field.Mode.NULLABLE, fields.get(0));
        assertBqField(protoField.getFields().get(1).getName(), LegacySQLTypeName.BYTES, Field.Mode.NULLABLE, fields.get(1));
        assertBqField(protoField.getFields().get(2).getName(), LegacySQLTypeName.RECORD, Field.Mode.NULLABLE, fields.get(2));
        assertBqField(protoField.getFields().get(3).getName(), LegacySQLTypeName.RECORD, Field.Mode.NULLABLE, fields.get(3));
        assertEquals(2, fields.get(2).getSubFields().size());
        assertBqField("duration_seconds", LegacySQLTypeName.INTEGER, Field.Mode.NULLABLE, fields.get(2).getSubFields().get(0));
        assertBqField("duration_nanos", LegacySQLTypeName.INTEGER, Field.Mode.NULLABLE, fields.get(2).getSubFields().get(1));

        assertEquals(3, fields.get(3).getSubFields().size());
        assertBqField("year", LegacySQLTypeName.INTEGER, Field.Mode.NULLABLE, fields.get(3).getSubFields().get(0));
        assertBqField("month", LegacySQLTypeName.INTEGER, Field.Mode.NULLABLE, fields.get(3).getSubFields().get(1));
        assertBqField("day", LegacySQLTypeName.INTEGER, Field.Mode.NULLABLE, fields.get(3).getSubFields().get(2));
    }

    @Test
    public void shouldTestConvertToSchemaForRepeatedFields() {
        ProtoField protoField = TestProtoUtil.createProtoField(new ArrayList<ProtoField>() {{
            add(TestProtoUtil.createProtoField("field1_map",
                    DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32,
                    DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED));
            add(TestProtoUtil.createProtoField("field2_repeated",
                    DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING,
                    DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED));

        }});

        List<Field> fields = BigqueryFields.generateBigquerySchema(protoField);

        assertEquals(protoField.getFields().size(), fields.size());
        assertBqField(protoField.getFields().get(0).getName(), LegacySQLTypeName.INTEGER, Field.Mode.REPEATED, fields.get(0));
        assertBqField(protoField.getFields().get(1).getName(), LegacySQLTypeName.STRING, Field.Mode.REPEATED, fields.get(1));
    }

    public void assertMultipleFields(List<ProtoField> pfields, List<Field> bqFields) {
        IntStream.range(0, bqFields.size())
                .forEach(index -> {
                    assertBqField(pfields.get(index).getName(), expectedType.get(pfields.get(index).getType()), Field.Mode.NULLABLE, bqFields.get(index));
                });
    }

    public void assertBqField(String name, LegacySQLTypeName ftype, Field.Mode mode, Field bqf) {
        assertEquals(mode, bqf.getMode());
        assertEquals(name, bqf.getName());
        assertEquals(ftype, bqf.getType());
    }


}
