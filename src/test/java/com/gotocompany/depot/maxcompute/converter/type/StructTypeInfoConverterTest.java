package com.gotocompany.depot.maxcompute.converter.type;

import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.protobuf.Descriptors;
import com.gotocompany.depot.TestMaxComputeTypeInfo;
import org.junit.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class StructTypeInfoConverterTest {

    private static final int STRUCT_INDEX = 4;
    private static final Descriptors.Descriptor DESCRIPTOR = TestMaxComputeTypeInfo.TestRoot.getDescriptor();
    private final StructTypeInfoConverter structTypeInfoConverter = new StructTypeInfoConverter();

    @Test
    public void shouldConvertToStringTypeInfo() {
        TypeInfo typeInfo = structTypeInfoConverter.convert(DESCRIPTOR.getFields().get(STRUCT_INDEX));

        assertEquals(TypeInfoFactory.STRING, typeInfo);
    }

    @Test
    public void shouldReturnTrueWhenCanConvertIsCalledWithStructFieldDescriptor() {
        assertTrue(structTypeInfoConverter.canConvert(DESCRIPTOR.getFields().get(STRUCT_INDEX)));
    }

    @Test
    public void shouldReturnFalseWhenCanConvertIsCalledWithNonStructFieldDescriptor() {
        assertFalse(structTypeInfoConverter.canConvert(DESCRIPTOR.getFields().get(0)));
        assertFalse(structTypeInfoConverter.canConvert(DESCRIPTOR.getFields().get(1)));
    }
}
