package com.gotocompany.depot.maxcompute.converter.type;

import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.protobuf.Descriptors;
import com.gotocompany.depot.TestMaxComputeTypeInfo;
import org.junit.jupiter.api.Assertions;
import org.junit.Test;

public class PrimitiveTypeInfoConverterTest {

    private final PrimitiveTypeInfoConverter primitiveTypeInfoConverter = new PrimitiveTypeInfoConverter();
    private final Descriptors.Descriptor descriptor = TestMaxComputeTypeInfo.TestFields.getDescriptor();

    @Test
    public void shouldConvertToBinary() {
        TypeInfo typeInfo = primitiveTypeInfoConverter.convert(descriptor.findFieldByName("bytes_field"));

        Assertions.assertEquals(TypeInfoFactory.BINARY, typeInfo);
    }

    @Test
    public void shouldConvertToString() {
        TypeInfo typeInfo = primitiveTypeInfoConverter.convert(descriptor.findFieldByName("string_field"));

        Assertions.assertEquals(TypeInfoFactory.STRING, typeInfo);
    }

    @Test
    public void shouldConvertEnumToString() {
        TypeInfo typeInfo = primitiveTypeInfoConverter.convert(descriptor.findFieldByName("enum_field"));

        Assertions.assertEquals(TypeInfoFactory.STRING, typeInfo);
    }

    @Test
    public void shouldConvertToDouble() {
        TypeInfo typeInfo = primitiveTypeInfoConverter.convert(descriptor.findFieldByName("double_field"));

        Assertions.assertEquals(TypeInfoFactory.DOUBLE, typeInfo);
    }

    @Test
    public void shouldConvertToFloat() {
        TypeInfo typeInfo = primitiveTypeInfoConverter.convert(descriptor.findFieldByName("float_field"));

        Assertions.assertEquals(TypeInfoFactory.FLOAT, typeInfo);
    }

    @Test
    public void shouldConvertToBoolean() {
        TypeInfo typeInfo = primitiveTypeInfoConverter.convert(descriptor.findFieldByName("bool_field"));

        Assertions.assertEquals(TypeInfoFactory.BOOLEAN, typeInfo);
    }

    @Test
    public void shouldConvertToBigInt() {
        TypeInfo typeInfo = primitiveTypeInfoConverter.convert(descriptor.findFieldByName("int64_field"));

        Assertions.assertEquals(TypeInfoFactory.BIGINT, typeInfo);
    }

    @Test
    public void shouldConvertUInt64ToBigInt() {
        TypeInfo typeInfo = primitiveTypeInfoConverter.convert(descriptor.findFieldByName("uint64_field"));

        Assertions.assertEquals(TypeInfoFactory.BIGINT, typeInfo);
    }

    @Test
    public void shouldConvertToInt() {
        TypeInfo typeInfo = primitiveTypeInfoConverter.convert(descriptor.findFieldByName("int32_field"));

        Assertions.assertEquals(TypeInfoFactory.INT, typeInfo);
    }

    @Test
    public void shouldConvertUInt32ToInt() {
        TypeInfo typeInfo = primitiveTypeInfoConverter.convert(descriptor.findFieldByName("uint32_field"));

        Assertions.assertEquals(TypeInfoFactory.INT, typeInfo);
    }

    @Test
    public void shouldConvertFixed64ToBigInt() {
        TypeInfo typeInfo = primitiveTypeInfoConverter.convert(descriptor.findFieldByName("fixed64_field"));

        Assertions.assertEquals(TypeInfoFactory.BIGINT, typeInfo);
    }

    @Test
    public void shouldConvertFixed32ToInt() {
        TypeInfo typeInfo = primitiveTypeInfoConverter.convert(descriptor.findFieldByName("fixed32_field"));

        Assertions.assertEquals(TypeInfoFactory.INT, typeInfo);
    }

    @Test
    public void shouldConvertSFixed32ToInt() {
        TypeInfo typeInfo = primitiveTypeInfoConverter.convert(descriptor.findFieldByName("sfixed32_field"));

        Assertions.assertEquals(TypeInfoFactory.INT, typeInfo);
    }

    @Test
    public void shouldConvertSFixed64ToBigInt() {
        TypeInfo typeInfo = primitiveTypeInfoConverter.convert(descriptor.findFieldByName("sfixed64_field"));

        Assertions.assertEquals(TypeInfoFactory.BIGINT, typeInfo);
    }

    @Test
    public void shouldConvertSInt32ToInt() {
        TypeInfo typeInfo = primitiveTypeInfoConverter.convert(descriptor.findFieldByName("sint32_field"));

        Assertions.assertEquals(TypeInfoFactory.INT, typeInfo);
    }

    @Test
    public void shouldConvertSInt64ToBigInt() {
        TypeInfo typeInfo = primitiveTypeInfoConverter.convert(descriptor.findFieldByName("sint64_field"));

        Assertions.assertEquals(TypeInfoFactory.BIGINT, typeInfo);
    }
}
