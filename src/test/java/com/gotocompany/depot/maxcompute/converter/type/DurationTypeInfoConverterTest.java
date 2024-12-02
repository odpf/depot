package com.gotocompany.depot.maxcompute.converter.type;

import com.aliyun.odps.type.TypeInfo;
import com.google.protobuf.Descriptors;
import com.gotocompany.depot.TestMaxComputeTypeInfo;
import org.junit.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DurationTypeInfoConverterTest {

    private static final int DURATION_INDEX = 5;
    private final Descriptors.Descriptor descriptor = TestMaxComputeTypeInfo.TestRoot.getDescriptor();
    private final DurationTypeInfoConverter durationTypeInfoConverter = new DurationTypeInfoConverter();

    @Test
    public void shouldConvertToStruct() {
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.getFields().get(DURATION_INDEX);

        TypeInfo typeInfo = durationTypeInfoConverter.convert(fieldDescriptor);

        assertEquals("STRUCT<`seconds`:BIGINT,`nanos`:INT>", typeInfo.getTypeName());
    }

    @Test
    public void shouldReturnTrueForDuration() {
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.getFields().get(DURATION_INDEX);

        boolean canConvert = durationTypeInfoConverter.canConvert(fieldDescriptor);

        assertTrue(canConvert);
    }

    @Test
    public void shouldReturnFalseForNonDuration() {
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.getFields().get(0);

        boolean canConvert = durationTypeInfoConverter.canConvert(fieldDescriptor);

        assertFalse(canConvert);
    }

}
