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

public class MessageTypeInfoConverterTest {

    private final Descriptors.Descriptor descriptor = TestMaxComputeTypeInfo.TestRoot.getDescriptor();
    private MessageTypeInfoConverter messageTypeInfoConverter;

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
        messageTypeInfoConverter = new MessageTypeInfoConverter(new ArrayList<>());
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
        List<TypeInfoConverter> converters = new ArrayList<>();
        converters.add(new PrimitiveTypeInfoConverter());
        converters.add(new DurationTypeInfoConverter());
        converters.add(new StructTypeInfoConverter());
        converters.add(new TimestampTypeInfoConverter());
        messageTypeInfoConverter = new MessageTypeInfoConverter(converters);
        converters.add(messageTypeInfoConverter);
    }

}
