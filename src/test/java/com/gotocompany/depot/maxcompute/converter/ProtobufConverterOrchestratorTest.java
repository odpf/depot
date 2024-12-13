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
import com.gotocompany.depot.maxcompute.model.MaxComputeProtobufConverterCache;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.when;

public class ProtobufConverterOrchestratorTest {

    private final Descriptors.Descriptor descriptor = TestMaxComputeTypeInfo.TestRoot.getDescriptor();
    private ProtobufConverterOrchestrator protobufConverterOrchestrator;

    @Before
    public void init() {
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        Mockito.when(maxComputeSinkConfig.getZoneId()).thenReturn(ZoneId.of("UTC"));
        when(maxComputeSinkConfig.getValidMinTimestamp()).thenReturn(LocalDateTime.parse("1970-01-01T00:00:00", DateTimeFormatter.ISO_DATE_TIME));
        when(maxComputeSinkConfig.getValidMaxTimestamp()).thenReturn(LocalDateTime.parse("9999-01-01T23:59:59", DateTimeFormatter.ISO_DATE_TIME));
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

        TypeInfo stringTypeInfo = protobufConverterOrchestrator.toMaxComputeTypeInfo(descriptor.findFieldByName("string_field"));
        TypeInfo messageTypeInfo = protobufConverterOrchestrator.toMaxComputeTypeInfo(descriptor.findFieldByName("inner_field"));
        TypeInfo repeatedTypeInfo = protobufConverterOrchestrator.toMaxComputeTypeInfo(descriptor.findFieldByName("inner_list_field"));
        TypeInfo timestampTypeInfo = protobufConverterOrchestrator.toMaxComputeTypeInfo(descriptor.findFieldByName("timestamp_field"));
        TypeInfo durationTypeInfo = protobufConverterOrchestrator.toMaxComputeTypeInfo(descriptor.findFieldByName("duration_field"));
        TypeInfo structTypeInfo = protobufConverterOrchestrator.toMaxComputeTypeInfo(descriptor.findFieldByName("struct_field"));

        assertEquals(expectedStringTypeInfoRepresentation, stringTypeInfo.toString());
        assertEquals(expectedMessageTypeRepresentation, messageTypeInfo.toString());
        assertEquals(expectedRepeatedMessageTypeRepresentation, repeatedTypeInfo.toString());
        assertEquals(expectedTimestampTypeInfoRepresentation, timestampTypeInfo.toString());
        assertEquals(expectedDurationTypeInfoRepresentation, durationTypeInfo.toString());
        assertEquals(expectedStructTypeInfoRepresentation, structTypeInfo.toString());
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIllegalArgumentExceptionForUnsupportedType() {
        Descriptors.FieldDescriptor unsupportedFieldDescriptor = descriptor.findFieldByName("empty_field");
        protobufConverterOrchestrator.toMaxComputeTypeInfo(unsupportedFieldDescriptor);
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

        Object stringRecord = protobufConverterOrchestrator.toMaxComputeValue(descriptor.findFieldByName("string_field"), messagePayload.getField(descriptor.findFieldByName("string_field")));
        Object messageRecord = protobufConverterOrchestrator.toMaxComputeValue(descriptor.findFieldByName("inner_field"), messagePayload.getField(descriptor.findFieldByName("inner_field")));
        Object repeatedMessageRecord = protobufConverterOrchestrator.toMaxComputeValue(descriptor.findFieldByName("inner_list_field"), messagePayload.getField(descriptor.findFieldByName("inner_list_field")));
        Object timestampRecord = protobufConverterOrchestrator.toMaxComputeValue(descriptor.findFieldByName("timestamp_field"), messagePayload.getField(descriptor.findFieldByName("timestamp_field")));
        Object durationRecord = protobufConverterOrchestrator.toMaxComputeValue(descriptor.findFieldByName("duration_field"), messagePayload.getField(descriptor.findFieldByName("duration_field")));
        Object structRecord = protobufConverterOrchestrator.toMaxComputeValue(descriptor.findFieldByName("struct_field"), messagePayload.getField(descriptor.findFieldByName("struct_field")));

        assertEquals("string_field", stringRecord);
        assertEquals(LocalDateTime.ofEpochSecond(100, 0, ZoneOffset.UTC), timestampRecord);
        assertEquals(new SimpleStruct(TypeInfoFactory.getStructTypeInfo(Arrays.asList("seconds", "nanos"), Arrays.asList(TypeInfoFactory.BIGINT, TypeInfoFactory.INT)), Arrays.asList(100L, 0)), durationRecord);
        assertEquals(expectedMessage, messageRecord);
        assertEquals(Collections.singletonList(expectedMessage), repeatedMessageRecord);
        assertEquals("{\"intField\":1.0,\"stringField\":\"String\"}", structRecord);
    }

    @Test
    public void shouldClearTheTypeInfoCache() throws NoSuchFieldException, IllegalAccessException {
        protobufConverterOrchestrator.toMaxComputeTypeInfo(descriptor.findFieldByName("inner_list_field"));
        Field field = protobufConverterOrchestrator.getClass()
                .getDeclaredField("maxComputeProtobufConverterCache");
        field.setAccessible(true);
        MaxComputeProtobufConverterCache maxComputeProtobufConverterCache = (MaxComputeProtobufConverterCache) field.get(protobufConverterOrchestrator);
        Field field1 = maxComputeProtobufConverterCache.getClass().getDeclaredField("typeInfoCache");
        field1.setAccessible(true);
        Map<String, TypeInfo> typeInfoCache = (Map<String, TypeInfo>) field1.get(maxComputeProtobufConverterCache);
        assertFalse((typeInfoCache).isEmpty());

        protobufConverterOrchestrator.clearCache();

        assertEquals(0, typeInfoCache.size());
    }
}
