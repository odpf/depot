package com.gotocompany.depot.maxcompute;

import com.aliyun.odps.type.TypeInfo;
import com.google.protobuf.Descriptors;
import com.gotocompany.depot.TestMaxComputeTypeInfo;
import com.gotocompany.depot.maxcompute.converter.DurationTypeInfoConverter;
import com.gotocompany.depot.maxcompute.converter.MessageTypeInfoConverter;
import com.gotocompany.depot.maxcompute.converter.PrimitiveTypeInfoConverter;
import com.gotocompany.depot.maxcompute.converter.StructTypeInfoConverter;
import com.gotocompany.depot.maxcompute.converter.TimestampTypeInfoConverter;
import com.gotocompany.depot.maxcompute.converter.TypeInfoConverter;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.util.ArrayList;
import java.util.List;

public class MessageTypeInfoConverterTest {

    private final Descriptors.Descriptor DESCRIPTOR = TestMaxComputeTypeInfo.TestRoot.getDescriptor();
    private MessageTypeInfoConverter messageTypeInfoConverter;

    @Before
    public void initialize() {
        initializeConverters();
    }

    @Test
    public void shouldConvertMessageToProperTypeInfo() {
        TypeInfo firstMessageFieldTypeInfo = messageTypeInfoConverter.convert(DESCRIPTOR.getFields().get(1));
        TypeInfo secondMessageFieldTypeInfo = messageTypeInfoConverter.convert(DESCRIPTOR.getFields().get(2));

        String expectedFirstMessageTypeRepresentation = "STRUCT<string_field:STRING,another_inner_field:STRUCT<string_field:STRING>,another_inner_list_field:ARRAY<STRUCT<string_field:STRING>>>";
        String expectedSecondMessageTypeRepresentation = String.format("ARRAY<%s>", expectedFirstMessageTypeRepresentation);

        Assertions.assertEquals(expectedFirstMessageTypeRepresentation, firstMessageFieldTypeInfo.toString());
        Assertions.assertEquals(expectedSecondMessageTypeRepresentation, secondMessageFieldTypeInfo.toString());
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIllegalArgumentExceptionWhenUnsupportedTypeIsGiven() {
        messageTypeInfoConverter = new MessageTypeInfoConverter(new ArrayList<>());
        Descriptors.FieldDescriptor unsupportedFieldDescriptor = DESCRIPTOR.getFields().get(1);

        messageTypeInfoConverter.convert(unsupportedFieldDescriptor);
    }

    @Test
    public void shouldReturnTrueWhenCanConvertIsCalledWithMessageFieldDescriptor() {
        Assertions.assertTrue(messageTypeInfoConverter.canConvert(DESCRIPTOR.getFields().get(1)));
    }

    @Test
    public void shouldReturnFalseWhenCanConvertIsCalledWithNonMessageFieldDescriptor() {
        Assertions.assertFalse(messageTypeInfoConverter.canConvert(DESCRIPTOR.getFields().get(0)));
    }

    @Test
    public void shouldReturnMinIntegerAsPriority() {
        Assertions.assertEquals(Integer.MIN_VALUE, messageTypeInfoConverter.getPriority());
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
