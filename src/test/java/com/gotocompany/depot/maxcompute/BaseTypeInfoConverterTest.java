package com.gotocompany.depot.maxcompute;

import com.aliyun.odps.type.TypeInfo;
import com.google.protobuf.Descriptors;
import com.gotocompany.depot.TestMaxComputeTypeInfo;
import com.gotocompany.depot.maxcompute.converter.BaseTypeInfoConverter;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

public class BaseTypeInfoConverterTest {

    private final Descriptors.Descriptor descriptor = TestMaxComputeTypeInfo.TestRoot.getDescriptor();
    private final BaseTypeInfoConverter baseTypeInfoConverter = new BaseTypeInfoConverter();

    @Test
    public void shouldConvertPayloadToTypeInfo() {
        String expectedStringTypeInfoRepresentation = "STRING";
        String expectedMessageTypeRepresentation = "STRUCT<string_field:STRING,another_inner_field:STRUCT<string_field:STRING>,another_inner_list_field:ARRAY<STRUCT<string_field:STRING>>>";
        String expectedRepeatedMessageTypeRepresentation = String.format("ARRAY<%s>", expectedMessageTypeRepresentation);
        String expectedTimestampTypeInfoRepresentation = "TIMESTAMP_NTZ";
        String expectedDurationTypeInfoRepresentation = "STRUCT<seconds:BIGINT,nanos:INT>";
        String expectedStructTypeInfoRepresentation = "STRING";

        TypeInfo stringTypeInfo = baseTypeInfoConverter.convert(descriptor.findFieldByName("string_field"));
        TypeInfo messageTypeInfo = baseTypeInfoConverter.convert(descriptor.findFieldByName("inner_field"));
        TypeInfo repeatedTypeInfo = baseTypeInfoConverter.convert(descriptor.findFieldByName("inner_list_field"));
        TypeInfo timestampTypeInfo = baseTypeInfoConverter.convert(descriptor.findFieldByName("timestamp_field"));
        TypeInfo durationTypeInfo = baseTypeInfoConverter.convert(descriptor.findFieldByName("duration_field"));
        TypeInfo structTypeInfo = baseTypeInfoConverter.convert(descriptor.findFieldByName("struct_field"));

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
        baseTypeInfoConverter.convert(unsupportedFieldDescriptor);
    }

}
