package com.gotocompany.depot.maxcompute.converter.type;

import com.aliyun.odps.type.TypeInfo;
import com.google.protobuf.Descriptors;
import com.gotocompany.depot.TestMaxComputeTypeInfo;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MessageProtobufTypeInfoConverterTest {

    private final Descriptors.Descriptor descriptor = TestMaxComputeTypeInfo.TestRoot.getDescriptor();
    private MessageProtobufTypeInfoConverter messageTypeInfoConverter;

    @Before
    public void initialize() {
        initializeConverters();
    }

    @Test
    public void shouldConvertMessageToProperTypeInfo() {
        TypeInfo firstMessageFieldTypeInfo = messageTypeInfoConverter.convert(descriptor.getFields().get(1));
        TypeInfo secondMessageFieldTypeInfo = messageTypeInfoConverter.convert(descriptor.getFields().get(2));

        String expectedFirstMessageTypeRepresentation = "STRUCT<`string_field`:STRING,`another_inner_field`:STRUCT<`string_field`:STRING>,`another_inner_list_field`:ARRAY<STRUCT<`string_field`:STRING>>>";
        String expectedSecondMessageTypeRepresentation = String.format("ARRAY<%s>", expectedFirstMessageTypeRepresentation);

        assertEquals(expectedFirstMessageTypeRepresentation, firstMessageFieldTypeInfo.toString());
        assertEquals(expectedSecondMessageTypeRepresentation, secondMessageFieldTypeInfo.toString());
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIllegalArgumentExceptionWhenUnsupportedTypeIsGiven() {
        messageTypeInfoConverter = new MessageProtobufTypeInfoConverter(new ArrayList<>());
        Descriptors.FieldDescriptor unsupportedFieldDescriptor = descriptor.getFields().get(1);

        messageTypeInfoConverter.convert(unsupportedFieldDescriptor);
    }

    @Test
    public void shouldReturnTrueWhenCanConvertIsCalledWithMessageFieldDescriptor() {
        assertTrue(messageTypeInfoConverter.canConvert(descriptor.getFields().get(1)));
    }

    @Test
    public void shouldReturnFalseWhenCanConvertIsCalledWithNonMessageFieldDescriptor() {
        assertFalse(messageTypeInfoConverter.canConvert(descriptor.getFields().get(0)));
    }

    private void initializeConverters() {
        List<ProtobufTypeInfoConverter> converters = new ArrayList<>();
        converters.add(new PrimitiveProtobufTypeInfoConverter());
        converters.add(new DurationProtobufTypeInfoConverter());
        converters.add(new StructProtobufTypeInfoConverter());
        converters.add(new TimestampProtobufTypeInfoConverter());
        messageTypeInfoConverter = new MessageProtobufTypeInfoConverter(converters);
        converters.add(messageTypeInfoConverter);
    }

}
