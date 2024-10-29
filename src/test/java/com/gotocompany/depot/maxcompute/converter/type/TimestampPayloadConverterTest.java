package com.gotocompany.depot.maxcompute.converter.type;

import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.protobuf.Descriptors;
import com.gotocompany.depot.TestMaxComputeTypeInfo;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

public class TimestampPayloadConverterTest {

    private static final int TIMESTAMP_INDEX = 3;
    private final Descriptors.Descriptor descriptor = TestMaxComputeTypeInfo.TestRoot.getDescriptor();
    private final TimestampTypeInfoConverter timestampTypeInfoConverter = new TimestampTypeInfoConverter();

    @Test
    public void shouldConvertToTimestampNtz() {
        TypeInfo typeInfo = timestampTypeInfoConverter.convert(descriptor.getFields().get(TIMESTAMP_INDEX));

        Assertions.assertEquals(TypeInfoFactory.TIMESTAMP, typeInfo);
    }

    @Test
    public void shouldReturnTrueWhenCanConvertIsCalledWithTimestampFieldDescriptor() {
        Assertions.assertTrue(timestampTypeInfoConverter.canConvert(descriptor.getFields().get(TIMESTAMP_INDEX)));
    }

    @Test
    public void shouldReturnFalseWhenCanConvertIsCalledWithNonTimestampFieldDescriptor() {
        Assertions.assertFalse(timestampTypeInfoConverter.canConvert(descriptor.getFields().get(0)));
        Assertions.assertFalse(timestampTypeInfoConverter.canConvert(descriptor.getFields().get(1)));
    }
}
