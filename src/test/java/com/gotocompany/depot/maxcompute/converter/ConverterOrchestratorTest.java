package com.gotocompany.depot.maxcompute.converter;

import com.aliyun.odps.data.SimpleStruct;
import com.aliyun.odps.type.StructTypeInfo;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Duration;
import com.google.protobuf.Message;
import com.google.protobuf.Struct;
import com.google.protobuf.Timestamp;
import com.google.protobuf.Value;

import com.gotocompany.depot.TestMaxComputeTypeInfo;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ConverterOrchestratorTest {

    private final Descriptors.Descriptor descriptor = TestMaxComputeTypeInfo.TestRoot.getDescriptor();
    private final ConverterOrchestrator converterOrchestrator = new ConverterOrchestrator();

    @Test
    public void shouldConvertPayloadToTypeInfo() {
        String expectedStringTypeInfoRepresentation = "STRING";
        String expectedMessageTypeRepresentation = "STRUCT<string_field:STRING,another_inner_field:STRUCT<string_field:STRING>,another_inner_list_field:ARRAY<STRUCT<string_field:STRING>>>";
        String expectedRepeatedMessageTypeRepresentation = String.format("ARRAY<%s>", expectedMessageTypeRepresentation);
        String expectedTimestampTypeInfoRepresentation = "TIMESTAMP";
        String expectedDurationTypeInfoRepresentation = "STRUCT<seconds:BIGINT,nanos:INT>";
        String expectedStructTypeInfoRepresentation = "STRING";

        TypeInfo stringTypeInfo = converterOrchestrator.convert(descriptor.findFieldByName("string_field"));
        TypeInfo messageTypeInfo = converterOrchestrator.convert(descriptor.findFieldByName("inner_field"));
        TypeInfo repeatedTypeInfo = converterOrchestrator.convert(descriptor.findFieldByName("inner_list_field"));
        TypeInfo timestampTypeInfo = converterOrchestrator.convert(descriptor.findFieldByName("timestamp_field"));
        TypeInfo durationTypeInfo = converterOrchestrator.convert(descriptor.findFieldByName("duration_field"));
        TypeInfo structTypeInfo = converterOrchestrator.convert(descriptor.findFieldByName("struct_field"));

        Assertions.assertEquals(expectedStringTypeInfoRepresentation, stringTypeInfo.toString());
        Assertions.assertEquals(expectedMessageTypeRepresentation, messageTypeInfo.toString());
        Assertions.assertEquals(expectedRepeatedMessageTypeRepresentation, repeatedTypeInfo.toString());
        Assertions.assertEquals(expectedTimestampTypeInfoRepresentation, timestampTypeInfo.toString());
        Assertions.assertEquals(expectedDurationTypeInfoRepresentation, durationTypeInfo.toString());
        Assertions.assertEquals(expectedStructTypeInfoRepresentation, structTypeInfo.toString());
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIllegalArgumentExceptionForUnsupportedType() {
        Descriptors.FieldDescriptor unsupportedFieldDescriptor = descriptor.findFieldByName("empty_field");
        converterOrchestrator.convert(unsupportedFieldDescriptor);
    }

    @Test
    public void shouldConvertPayloadToRecord() {
        Struct.Builder structBuilder = Struct.newBuilder();
        structBuilder.putFields("intField", Value.newBuilder().setNumberValue(1.0).build());
        structBuilder.putFields("stringField", Value.newBuilder().setStringValue("String").build());
        TestMaxComputeTypeInfo.TestAnotherInner testAnotherInner = TestMaxComputeTypeInfo.TestAnotherInner.newBuilder()
                .setStringField("inner_string_field")
                .build();
        TestMaxComputeTypeInfo.TestInner testInner = TestMaxComputeTypeInfo.TestInner.newBuilder()
                .setAnotherInnerField(testAnotherInner)
                .addAllAnotherInnerListField(Collections.singletonList(testAnotherInner))
                .setStringField("string_field")
                .build();
        Message messagePayload = TestMaxComputeTypeInfo.TestRoot.newBuilder()
                .setStringField("string_field")
                .setTimestampField(Timestamp.newBuilder()
                        .setSeconds(100)
                        .setNanos(0)
                        .build())
                .setDurationField(Duration.newBuilder()
                        .setSeconds(100)
                        .setNanos(0)
                        .build())
                .setStructField(structBuilder.build())
                .setInnerField(testInner)
                .addAllInnerListField(Collections.singletonList(testInner))
                .build();
        StructTypeInfo messageTypeInfo = TypeInfoFactory.getStructTypeInfo(
                Arrays.asList("string_field", "another_inner_field", "another_inner_list_field"),
                Arrays.asList(TypeInfoFactory.STRING, TypeInfoFactory.getStructTypeInfo(Collections.singletonList("string_field"), Collections.singletonList(TypeInfoFactory.STRING)),
                        TypeInfoFactory.getArrayTypeInfo(TypeInfoFactory.getStructTypeInfo(Collections.singletonList("string_field"), Collections.singletonList(TypeInfoFactory.STRING))))
        );
        List<Object> messageValues = Arrays.asList("string_field", new SimpleStruct(TypeInfoFactory.getStructTypeInfo(Collections.singletonList("string_field"), Collections.singletonList(TypeInfoFactory.STRING)), Collections.singletonList("inner_string_field")),
                Collections.singletonList(new SimpleStruct(TypeInfoFactory.getStructTypeInfo(Collections.singletonList("string_field"), Collections.singletonList(TypeInfoFactory.STRING)), Collections.singletonList("inner_string_field"))));
        SimpleStruct expectedMessage = new SimpleStruct(messageTypeInfo, messageValues);

        Object stringRecord = converterOrchestrator.convert(descriptor.findFieldByName("string_field"), messagePayload.getField(descriptor.findFieldByName("string_field")));
        Object messageRecord = converterOrchestrator.convert(descriptor.findFieldByName("inner_field"), messagePayload.getField(descriptor.findFieldByName("inner_field")));
        Object repeatedMessageRecord = converterOrchestrator.convert(descriptor.findFieldByName("inner_list_field"), messagePayload.getField(descriptor.findFieldByName("inner_list_field")));
        Object timestampRecord = converterOrchestrator.convert(descriptor.findFieldByName("timestamp_field"), messagePayload.getField(descriptor.findFieldByName("timestamp_field")));
        Object durationRecord = converterOrchestrator.convert(descriptor.findFieldByName("duration_field"), messagePayload.getField(descriptor.findFieldByName("duration_field")));
        Object structRecord = converterOrchestrator.convert(descriptor.findFieldByName("struct_field"), messagePayload.getField(descriptor.findFieldByName("struct_field")));

        Assertions.assertEquals("string_field", stringRecord);
        Assertions.assertEquals(new java.sql.Timestamp(100 * 1000), timestampRecord);
        Assertions.assertEquals(new SimpleStruct(TypeInfoFactory.getStructTypeInfo(Arrays.asList("seconds", "nanos"), Arrays.asList(TypeInfoFactory.BIGINT, TypeInfoFactory.INT)), Arrays.asList(100L, 0)), durationRecord);
        Assertions.assertEquals(expectedMessage, messageRecord);
        Assertions.assertEquals(Collections.singletonList(expectedMessage), repeatedMessageRecord);
        Assertions.assertEquals("{\"intField\":1.0,\"stringField\":\"String\"}", structRecord);
    }

    @Test
    public void shouldClearTheTypeInfoCache() throws NoSuchFieldException, IllegalAccessException {
        converterOrchestrator.convert(descriptor.findFieldByName("inner_list_field"));
        Field field = converterOrchestrator.getClass()
                .getDeclaredField("typeInfoCache");
        field.setAccessible(true);
        Assertions.assertEquals(1, ((Map<String, TypeInfo>) field.get(converterOrchestrator)).size());

        converterOrchestrator.clearCache();

        Assertions.assertEquals(0, ((Map<String, TypeInfo>) field.get(converterOrchestrator)).size());
    }
}
