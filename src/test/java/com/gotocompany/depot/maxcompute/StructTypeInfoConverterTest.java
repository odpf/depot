package com.gotocompany.depot.maxcompute;

import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.protobuf.Descriptors;
import com.gotocompany.depot.TestMaxComputeTypeInfo;
import com.gotocompany.depot.maxcompute.converter.StructTypeInfoConverter;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

public class StructTypeInfoConverterTest {

    private final int STRUCT_INDEX = 4;
    private final Descriptors.Descriptor DESCRIPTOR = TestMaxComputeTypeInfo.TestRoot.getDescriptor();
    private final StructTypeInfoConverter structTypeInfoConverter = new StructTypeInfoConverter();

    @Test
    public void shouldConvertToStringTypeInfo() {
        TypeInfo typeInfo = structTypeInfoConverter.convert(DESCRIPTOR.getFields().get(STRUCT_INDEX));

        Assertions.assertEquals(TypeInfoFactory.STRING, typeInfo);
    }

    @Test
    public void shouldReturnTrueWhenCanConvertIsCalledWithStructFieldDescriptor() {
        Assertions.assertTrue(structTypeInfoConverter.canConvert(DESCRIPTOR.getFields().get(STRUCT_INDEX)));
    }

    @Test
    public void shouldReturnFalseWhenCanConvertIsCalledWithNonStructFieldDescriptor() {
        Assertions.assertFalse(structTypeInfoConverter.canConvert(DESCRIPTOR.getFields().get(0)));
        Assertions.assertFalse(structTypeInfoConverter.canConvert(DESCRIPTOR.getFields().get(1)));
    }
}
