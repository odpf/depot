package com.gotocompany.depot.maxcompute.converter.type;

import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.protobuf.Descriptors;
import com.gotocompany.depot.TestMaxComputeTypeInfo;
import org.junit.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TimestampPayloadConverterTest {

    private static final int TIMESTAMP_INDEX = 3;
    private final Descriptors.Descriptor descriptor = TestMaxComputeTypeInfo.TestRoot.getDescriptor();
    private final TimestampTypeInfoConverter timestampTypeInfoConverter = new TimestampTypeInfoConverter();

    @Test
    public void shouldConvertToTimestampNtz() {
        TypeInfo typeInfo = timestampTypeInfoConverter.convert(descriptor.getFields().get(TIMESTAMP_INDEX));

        assertEquals(TypeInfoFactory.TIMESTAMP_NTZ, typeInfo);
    }

    @Test
    public void shouldReturnTrueWhenCanConvertIsCalledWithTimestampFieldDescriptor() {
        assertTrue(timestampTypeInfoConverter.canConvert(descriptor.getFields().get(TIMESTAMP_INDEX)));
    }

    @Test
    public void shouldReturnFalseWhenCanConvertIsCalledWithNonTimestampFieldDescriptor() {
        assertFalse(timestampTypeInfoConverter.canConvert(descriptor.getFields().get(0)));
        assertFalse(timestampTypeInfoConverter.canConvert(descriptor.getFields().get(1)));
    }
}
