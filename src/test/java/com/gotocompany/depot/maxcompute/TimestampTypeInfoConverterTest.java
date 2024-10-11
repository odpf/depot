package com.gotocompany.depot.maxcompute;

import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.protobuf.Descriptors;
import com.gotocompany.depot.TestMaxComputeTypeInfo;
import com.gotocompany.depot.maxcompute.converter.TimestampTypeInfoConverter;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

public class TimestampTypeInfoConverterTest {

    private final int TIMESTAMP_INDEX = 3;
    private final Descriptors.Descriptor descriptor = TestMaxComputeTypeInfo.TestRoot.getDescriptor();
    private final TimestampTypeInfoConverter timestampTypeInfoConverter = new TimestampTypeInfoConverter();

    @Test
    public void shouldConvertToTimestampNtz() {
        TypeInfo typeInfo = timestampTypeInfoConverter.convert(descriptor.getFields().get(TIMESTAMP_INDEX));

        Assertions.assertEquals(TypeInfoFactory.TIMESTAMP_NTZ, typeInfo);
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
