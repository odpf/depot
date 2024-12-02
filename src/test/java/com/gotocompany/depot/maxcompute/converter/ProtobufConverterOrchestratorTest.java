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
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ProtobufConverterOrchestratorTest {

    private final Descriptors.Descriptor descriptor = TestMaxComputeTypeInfo.TestRoot.getDescriptor();
    private ProtobufConverterOrchestrator protobufConverterOrchestrator;

    @Before
    public void init() {
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        Mockito.when(maxComputeSinkConfig.getZoneId()).thenReturn(ZoneId.of("UTC"));
        protobufConverterOrchestrator = new ProtobufConverterOrchestrator(maxComputeSinkConfig);
    }

    @Test
    public void shouldConvertPayloadToTypeInfo() {
        String expectedStringTypeInfoRepresentation = "STRING";
        String expectedMessageTypeRepresentation = "STRUCT<`string_field`:STRING,`another_inner_field`:STRUCT<`string_field`:STRING>,`another_inner_list_field`:ARRAY<STRUCT<`string_field`:STRING>>>";
        String expectedRepeatedMessageTypeRepresentation = String.format("ARRAY<%s>", expectedMessageTypeRepresentation);
        String expectedTimestampTypeInfoRepresentation = "TIMESTAMP_NTZ";
        String expectedDurationTypeInfoRepresentation = "STRUCT<`seconds`:BIGINT,`nanos`:INT>";
        String expectedStructTypeInfoRepresentation = "STRING";

        TypeInfo stringTypeInfo = protobufConverterOrchestrator.convert(descriptor.findFieldByName("string_field"));
        TypeInfo messageTypeInfo = protobufConverterOrchestrator.convert(descriptor.findFieldByName("inner_field"));
        TypeInfo repeatedTypeInfo = protobufConverterOrchestrator.convert(descriptor.findFieldByName("inner_list_field"));
        TypeInfo timestampTypeInfo = protobufConverterOrchestrator.convert(descriptor.findFieldByName("timestamp_field"));
        TypeInfo durationTypeInfo = protobufConverterOrchestrator.convert(descriptor.findFieldByName("duration_field"));
        TypeInfo structTypeInfo = protobufConverterOrchestrator.convert(descriptor.findFieldByName("struct_field"));

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
        protobufConverterOrchestrator.convert(unsupportedFieldDescriptor);
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

        Object stringRecord = protobufConverterOrchestrator.convert(descriptor.findFieldByName("string_field"), messagePayload.getField(descriptor.findFieldByName("string_field")));
        Object messageRecord = protobufConverterOrchestrator.convert(descriptor.findFieldByName("inner_field"), messagePayload.getField(descriptor.findFieldByName("inner_field")));
        Object repeatedMessageRecord = protobufConverterOrchestrator.convert(descriptor.findFieldByName("inner_list_field"), messagePayload.getField(descriptor.findFieldByName("inner_list_field")));
        Object timestampRecord = protobufConverterOrchestrator.convert(descriptor.findFieldByName("timestamp_field"), messagePayload.getField(descriptor.findFieldByName("timestamp_field")));
        Object durationRecord = protobufConverterOrchestrator.convert(descriptor.findFieldByName("duration_field"), messagePayload.getField(descriptor.findFieldByName("duration_field")));
        Object structRecord = protobufConverterOrchestrator.convert(descriptor.findFieldByName("struct_field"), messagePayload.getField(descriptor.findFieldByName("struct_field")));

        Assertions.assertEquals("string_field", stringRecord);
        Assertions.assertEquals(LocalDateTime.ofEpochSecond(100, 0, ZoneOffset.UTC), timestampRecord);
        Assertions.assertEquals(new SimpleStruct(TypeInfoFactory.getStructTypeInfo(Arrays.asList("seconds", "nanos"), Arrays.asList(TypeInfoFactory.BIGINT, TypeInfoFactory.INT)), Arrays.asList(100L, 0)), durationRecord);
        Assertions.assertEquals(expectedMessage, messageRecord);
        Assertions.assertEquals(Collections.singletonList(expectedMessage), repeatedMessageRecord);
        Assertions.assertEquals("{\"intField\":1.0,\"stringField\":\"String\"}", structRecord);
    }

    @Test
    public void shouldClearTheTypeInfoCache() throws NoSuchFieldException, IllegalAccessException {
        protobufConverterOrchestrator.convert(descriptor.findFieldByName("inner_list_field"));
        Field field = protobufConverterOrchestrator.getClass()
                .getDeclaredField("typeInfoCache");
        field.setAccessible(true);
        Assertions.assertEquals(1, ((Map<String, TypeInfo>) field.get(protobufConverterOrchestrator)).size());

        protobufConverterOrchestrator.clearCache();

        Assertions.assertEquals(0, ((Map<String, TypeInfo>) field.get(protobufConverterOrchestrator)).size());
    }
}
