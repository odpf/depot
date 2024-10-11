package com.gotocompany.depot.maxcompute;

import com.aliyun.odps.type.TypeInfo;
import com.google.protobuf.Descriptors;
import com.gotocompany.depot.TestMaxComputeTypeInfo;
import com.gotocompany.depot.maxcompute.converter.DurationTypeInfoConverter;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

public class DurationTypeInfoConverterTest {

    private final int DURATION_INDEX = 5;
    private final Descriptors.Descriptor descriptor = TestMaxComputeTypeInfo.TestRoot.getDescriptor();
    private final DurationTypeInfoConverter durationTypeInfoConverter = new DurationTypeInfoConverter();

    @Test
    public void shouldConvertToStruct(){
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.getFields().get(DURATION_INDEX);

        TypeInfo typeInfo = durationTypeInfoConverter.convert(fieldDescriptor);

        Assertions.assertEquals("STRUCT<seconds:BIGINT,nanos:INT>", typeInfo.getTypeName());
    }

    @Test
    public void shouldReturnTrueForDuration(){
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.getFields().get(DURATION_INDEX);

        boolean canConvert = durationTypeInfoConverter.canConvert(fieldDescriptor);

        Assertions.assertTrue(canConvert);
    }

    @Test
    public void shouldReturnFalseForNonDuration(){
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.getFields().get(0);

        boolean canConvert = durationTypeInfoConverter.canConvert(fieldDescriptor);

        Assertions.assertFalse(canConvert);
    }
    
}
