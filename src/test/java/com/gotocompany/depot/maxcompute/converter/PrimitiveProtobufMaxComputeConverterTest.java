package com.gotocompany.depot.maxcompute.converter;

import com.aliyun.odps.data.Binary;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.gotocompany.depot.TestMaxComputeTypeInfo;
import com.gotocompany.depot.exception.InvalidMessageException;
import com.gotocompany.depot.maxcompute.model.ProtoPayload;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PrimitiveProtobufMaxComputeConverterTest {

    private final PrimitiveProtobufMaxComputeConverter primitiveProtobufMaxComputeConverter = new PrimitiveProtobufMaxComputeConverter();
    private final Descriptors.Descriptor descriptor = TestMaxComputeTypeInfo.TestFields.getDescriptor();
    private final Descriptors.Descriptor descriptorRepeated = TestMaxComputeTypeInfo.TestFieldsRepeated.getDescriptor();

    @Test
    public void shouldConvertToBinary() {
        TypeInfo typeInfo = primitiveProtobufMaxComputeConverter.convertTypeInfo(descriptor.findFieldByName("bytes_field"));

        assertEquals(TypeInfoFactory.BINARY, typeInfo);
    }

    @Test
    public void shouldConvertToString() {
        TypeInfo typeInfo = primitiveProtobufMaxComputeConverter.convertTypeInfo(descriptor.findFieldByName("string_field"));

        assertEquals(TypeInfoFactory.STRING, typeInfo);
    }

    @Test
    public void shouldConvertEnumToString() {
        TypeInfo typeInfo = primitiveProtobufMaxComputeConverter.convertTypeInfo(descriptor.findFieldByName("enum_field"));

        assertEquals(TypeInfoFactory.STRING, typeInfo);
    }

    @Test
    public void shouldConvertToDouble() {
        TypeInfo typeInfo = primitiveProtobufMaxComputeConverter.convertTypeInfo(descriptor.findFieldByName("double_field"));

        assertEquals(TypeInfoFactory.DOUBLE, typeInfo);
    }

    @Test
    public void shouldConvertToFloat() {
        TypeInfo typeInfo = primitiveProtobufMaxComputeConverter.convertTypeInfo(descriptor.findFieldByName("float_field"));

        assertEquals(TypeInfoFactory.FLOAT, typeInfo);
    }

    @Test
    public void shouldConvertToBoolean() {
        TypeInfo typeInfo = primitiveProtobufMaxComputeConverter.convertTypeInfo(descriptor.findFieldByName("bool_field"));

        assertEquals(TypeInfoFactory.BOOLEAN, typeInfo);
    }

    @Test
    public void shouldConvertToBigInt() {
        TypeInfo typeInfo = primitiveProtobufMaxComputeConverter.convertTypeInfo(descriptor.findFieldByName("int64_field"));

        assertEquals(TypeInfoFactory.BIGINT, typeInfo);
    }

    @Test
    public void shouldConvertUInt64ToBigInt() {
        TypeInfo typeInfo = primitiveProtobufMaxComputeConverter.convertTypeInfo(descriptor.findFieldByName("uint64_field"));

        assertEquals(TypeInfoFactory.BIGINT, typeInfo);
    }

    @Test
    public void shouldConvertToInt() {
        TypeInfo typeInfo = primitiveProtobufMaxComputeConverter.convertTypeInfo(descriptor.findFieldByName("int32_field"));

        assertEquals(TypeInfoFactory.INT, typeInfo);
    }

    @Test
    public void shouldConvertUInt32ToInt() {
        TypeInfo typeInfo = primitiveProtobufMaxComputeConverter.convertTypeInfo(descriptor.findFieldByName("uint32_field"));

        assertEquals(TypeInfoFactory.INT, typeInfo);
    }

    @Test
    public void shouldConvertFixed64ToBigInt() {
        TypeInfo typeInfo = primitiveProtobufMaxComputeConverter.convertTypeInfo(descriptor.findFieldByName("fixed64_field"));

        assertEquals(TypeInfoFactory.BIGINT, typeInfo);
    }

    @Test
    public void shouldConvertFixed32ToInt() {
        TypeInfo typeInfo = primitiveProtobufMaxComputeConverter.convertTypeInfo(descriptor.findFieldByName("fixed32_field"));

        assertEquals(TypeInfoFactory.INT, typeInfo);
    }

    @Test
    public void shouldConvertSFixed32ToInt() {
        TypeInfo typeInfo = primitiveProtobufMaxComputeConverter.convertTypeInfo(descriptor.findFieldByName("sfixed32_field"));

        assertEquals(TypeInfoFactory.INT, typeInfo);
    }

    @Test
    public void shouldConvertSFixed64ToBigInt() {
        TypeInfo typeInfo = primitiveProtobufMaxComputeConverter.convertTypeInfo(descriptor.findFieldByName("sfixed64_field"));

        assertEquals(TypeInfoFactory.BIGINT, typeInfo);
    }

    @Test
    public void shouldConvertSInt32ToInt() {
        TypeInfo typeInfo = primitiveProtobufMaxComputeConverter.convertTypeInfo(descriptor.findFieldByName("sint32_field"));

        assertEquals(TypeInfoFactory.INT, typeInfo);
    }

    @Test
    public void shouldConvertSInt64ToBigInt() {
        TypeInfo typeInfo = primitiveProtobufMaxComputeConverter.convertTypeInfo(descriptor.findFieldByName("sint64_field"));

        assertEquals(TypeInfoFactory.BIGINT, typeInfo);
    }

    @Test
    public void shouldReturnObjectAsItIsWhenTypeIsByteArray() {
        byte[] bytes = "bytes".getBytes(StandardCharsets.UTF_8);
        Message message = TestMaxComputeTypeInfo.TestFields.newBuilder().setBytesField(ByteString.copyFrom(bytes)).build();

        Object result = primitiveProtobufMaxComputeConverter.convertPayload(new ProtoPayload(descriptor.getFields().get(0), message.getField(descriptor.getFields().get(0)), true));

        assertTrue(result instanceof Binary);
        assertArrayEquals(bytes, ((Binary) result).data());
    }

    @Test
    public void shouldReturnObjectAsItIsWhenTypeIsString() {
        String value = "test";
        Message message = TestMaxComputeTypeInfo.TestFields.newBuilder().setStringField(value).build();

        Object result = primitiveProtobufMaxComputeConverter.convertPayload(new ProtoPayload(descriptor.getFields().get(1), message.getField(descriptor.getFields().get(1)), true));

        assertTrue(result instanceof String);
        assertEquals(value, result);
    }

    @Test
    public void shouldReturnObjectAsItIsWhenTypeIsEnum() {
        TestMaxComputeTypeInfo.TestEnum value = TestMaxComputeTypeInfo.TestEnum.TEST_1;
        Message message = TestMaxComputeTypeInfo.TestFields.newBuilder().setEnumField(value).build();

        Object result = primitiveProtobufMaxComputeConverter.convertPayload(new ProtoPayload(descriptor.getFields().get(2), message.getField(descriptor.getFields().get(2)), true));

        assertTrue(result instanceof String);
        assertEquals(value.name(), result);
    }

    @Test
    public void shouldReturnObjectAsItIsWhenTypeIsDouble() {
        double value = 1.23;
        Message message = TestMaxComputeTypeInfo.TestFields.newBuilder().setDoubleField(value).build();

        Object result = primitiveProtobufMaxComputeConverter.convertPayload(new ProtoPayload(descriptor.getFields().get(3), message.getField(descriptor.getFields().get(3)), true));

        assertTrue(result instanceof Double);
        assertEquals(value, result);
    }

    @Test(expected = InvalidMessageException.class)
    public void shouldThrowInvalidMessageExceptionWhenDoublePositiveInfinity() {
        double value = Double.POSITIVE_INFINITY;
        Message message = TestMaxComputeTypeInfo.TestFields.newBuilder().setDoubleField(value).build();

        primitiveProtobufMaxComputeConverter.convertPayload(new ProtoPayload(descriptor.getFields().get(3), message.getField(descriptor.getFields().get(3)), true));
    }

    @Test(expected = InvalidMessageException.class)
    public void shouldThrowInvalidMessageExceptionWhenDoubleNegativeInfinity() {
        double value = Double.NEGATIVE_INFINITY;
        Message message = TestMaxComputeTypeInfo.TestFields.newBuilder().setDoubleField(value).build();

        primitiveProtobufMaxComputeConverter.convertPayload(new ProtoPayload(descriptor.getFields().get(3), message.getField(descriptor.getFields().get(3)), true));
    }

    @Test(expected = InvalidMessageException.class)
    public void shouldThrowInvalidMessageExceptionWhenDoubleNaN() {
        double value = Double.NaN;
        Message message = TestMaxComputeTypeInfo.TestFields.newBuilder().setDoubleField(value).build();

        primitiveProtobufMaxComputeConverter.convertPayload(new ProtoPayload(descriptor.getFields().get(3), message.getField(descriptor.getFields().get(3)), true));
    }

    @Test
    public void shouldReturnObjectAsItIsWhenTypeIsFloat() {
        float value = 1.23f;
        Message message = TestMaxComputeTypeInfo.TestFields.newBuilder().setFloatField(value).build();

        Object result = primitiveProtobufMaxComputeConverter.convertPayload(new ProtoPayload(descriptor.getFields().get(4), message.getField(descriptor.getFields().get(4)), true));

        assertTrue(result instanceof Float);
        assertEquals(value, result);
    }

    @Test(expected = InvalidMessageException.class)
    public void shouldThrowInvalidMessageExceptionWhenFloatPositiveInfinity() {
        float value = Float.POSITIVE_INFINITY;
        Message message = TestMaxComputeTypeInfo.TestFields.newBuilder().setFloatField(value).build();

        primitiveProtobufMaxComputeConverter.convertPayload(new ProtoPayload(descriptor.getFields().get(4), message.getField(descriptor.getFields().get(4)), true));
    }

    @Test(expected = InvalidMessageException.class)
    public void shouldThrowInvalidMessageExceptionWhenFloatNegativeInfinity() {
        float value = Float.NEGATIVE_INFINITY;
        Message message = TestMaxComputeTypeInfo.TestFields.newBuilder().setFloatField(value).build();

        primitiveProtobufMaxComputeConverter.convertPayload(new ProtoPayload(descriptor.getFields().get(4), message.getField(descriptor.getFields().get(4)), true));
    }

    @Test(expected = InvalidMessageException.class)
    public void shouldThrowInvalidMessageExceptionWhenFloatNaN() {
        float value = Float.NaN;
        Message message = TestMaxComputeTypeInfo.TestFields.newBuilder().setFloatField(value).build();

        primitiveProtobufMaxComputeConverter.convertPayload(new ProtoPayload(descriptor.getFields().get(4), message.getField(descriptor.getFields().get(4)), true));
    }

    @Test
    public void shouldReturnObjectAsItIsWhenTypeIsBoolean() {
        boolean value = true;
        Message message = TestMaxComputeTypeInfo.TestFields.newBuilder().setBoolField(value).build();

        Object result = primitiveProtobufMaxComputeConverter.convertPayload(new ProtoPayload(descriptor.getFields().get(5), message.getField(descriptor.getFields().get(5)), true));

        assertTrue(result instanceof Boolean);
        assertEquals(value, result);
    }

    @Test
    public void shouldReturnObjectAsItIsWhenTypeIsInt64() {
        long value = 123L;
        Message message = TestMaxComputeTypeInfo.TestFields.newBuilder().setInt64Field(value).build();

        Object result = primitiveProtobufMaxComputeConverter.convertPayload(new ProtoPayload(descriptor.getFields().get(6), message.getField(descriptor.getFields().get(6)), true));

        assertTrue(result instanceof Long);
        assertEquals(value, result);
    }

    @Test
    public void shouldReturnObjectAsItIsWhenTypeIsUInt64() {
        long value = 123L;
        Message message = TestMaxComputeTypeInfo.TestFields.newBuilder().setUint64Field(value).build();

        Object result = primitiveProtobufMaxComputeConverter.convertPayload(new ProtoPayload(descriptor.getFields().get(7), message.getField(descriptor.getFields().get(7)), true));

        assertTrue(result instanceof Long);
        assertEquals(value, result);
    }

    @Test
    public void shouldReturnObjectAsItIsWhenTypeIsInt32() {
        int value = 123;
        Message message = TestMaxComputeTypeInfo.TestFields.newBuilder().setInt32Field(value).build();

        Object result = primitiveProtobufMaxComputeConverter.convertPayload(new ProtoPayload(descriptor.getFields().get(8), message.getField(descriptor.getFields().get(8)), true));

        assertTrue(result instanceof Integer);
        assertEquals(value, result);
    }

    @Test
    public void shouldReturnObjectAsItIsWhenTypeIsUInt32() {
        int value = 123;
        Message message = TestMaxComputeTypeInfo.TestFields.newBuilder().setUint32Field(value).build();

        Object result = primitiveProtobufMaxComputeConverter.convertPayload(new ProtoPayload(descriptor.getFields().get(9), message.getField(descriptor.getFields().get(9)), true));

        assertTrue(result instanceof Integer);
        assertEquals(value, result);
    }

    @Test
    public void shouldReturnObjectAsItIsWhenTypeIsFixed64() {
        long value = 123L;
        Message message = TestMaxComputeTypeInfo.TestFields.newBuilder().setFixed64Field(value).build();

        Object result = primitiveProtobufMaxComputeConverter.convertPayload(new ProtoPayload(descriptor.getFields().get(10), message.getField(descriptor.getFields().get(10)), true));

        assertTrue(result instanceof Long);
        assertEquals(value, result);
    }

    @Test
    public void shouldReturnObjectAsItIsWhenTypeIsFixed32() {
        int value = 123;
        Message message = TestMaxComputeTypeInfo.TestFields.newBuilder().setFixed32Field(value).build();

        Object result = primitiveProtobufMaxComputeConverter.convertPayload(new ProtoPayload(descriptor.getFields().get(11), message.getField(descriptor.getFields().get(11)), true));

        assertTrue(result instanceof Integer);
        assertEquals(value, result);
    }

    @Test
    public void shouldReturnObjectAsItIsWhenTypeIsSFixed32() {
        int value = 123;
        Message message = TestMaxComputeTypeInfo.TestFields.newBuilder().setSfixed32Field(value).build();

        Object result = primitiveProtobufMaxComputeConverter.convertPayload(new ProtoPayload(descriptor.getFields().get(12), message.getField(descriptor.getFields().get(12)), true));

        assertTrue(result instanceof Integer);
        assertEquals(value, result);
    }

    @Test
    public void shouldReturnObjectAsItIsWhenTypeIsSFixed64() {
        long value = 123L;
        Message message = TestMaxComputeTypeInfo.TestFields.newBuilder().setSfixed64Field(value).build();

        Object result = primitiveProtobufMaxComputeConverter.convertPayload(new ProtoPayload(descriptor.getFields().get(13), message.getField(descriptor.getFields().get(13)), true));

        assertTrue(result instanceof Long);
        assertEquals(value, result);
    }

    @Test
    public void shouldReturnObjectAsItIsWhenTypeIsSInt32() {
        int value = 123;
        Message message = TestMaxComputeTypeInfo.TestFields.newBuilder().setSint32Field(value).build();

        Object result = primitiveProtobufMaxComputeConverter.convertPayload(new ProtoPayload(descriptor.getFields().get(14), message.getField(descriptor.getFields().get(14)), true));

        assertTrue(result instanceof Integer);
        assertEquals(value, result);
    }

    @Test
    public void shouldReturnObjectAsItIsWhenTypeIsSInt64() {
        long value = 123L;
        Message message = TestMaxComputeTypeInfo.TestFields.newBuilder().setSint64Field(value).build();

        Object result = primitiveProtobufMaxComputeConverter.convertPayload(new ProtoPayload(descriptor.getFields().get(15), message.getField(descriptor.getFields().get(15)), true));

        assertTrue(result instanceof Long);
        assertEquals(value, result);
    }

    @Test
    public void shouldReturnListObjectAsItIsWhenTypeIsByteArrayList() {
        byte[] bytes = "bytes".getBytes(StandardCharsets.UTF_8);
        Message message = TestMaxComputeTypeInfo.TestFieldsRepeated.newBuilder().addAllBytesFields(Collections.singletonList(ByteString.copyFrom(bytes))).build();

        Object result = primitiveProtobufMaxComputeConverter.convertPayload(new ProtoPayload(descriptorRepeated.getFields().get(0), message.getField(descriptorRepeated.getFields().get(0)), true));

        assertTrue(result instanceof List<?>);
        assertTrue(((List<?>) result).stream().allMatch(element -> element instanceof Binary));
    }

    @Test
    public void shouldReturnListObjectAsItIsWhenTypeIsStringList() {
        String value = "test";
        Message message = TestMaxComputeTypeInfo.TestFieldsRepeated.newBuilder().addAllStringFields(Collections.singletonList(value)).build();

        Object result = primitiveProtobufMaxComputeConverter.convertPayload(new ProtoPayload(descriptorRepeated.getFields().get(1), message.getField(descriptorRepeated.getFields().get(1)), true));

        assertTrue(result instanceof List<?>);
        assertTrue(((List<?>) result).stream().allMatch(element -> element instanceof String));
    }

    @Test
    public void shouldReturnListObjectAsItIsWhenTypeIsEnumList() {
        TestMaxComputeTypeInfo.TestEnum value = TestMaxComputeTypeInfo.TestEnum.TEST_1;
        Message message = TestMaxComputeTypeInfo.TestFieldsRepeated.newBuilder().addAllEnumFields(Collections.singletonList(value)).build();

        Object result = primitiveProtobufMaxComputeConverter.convertPayload(new ProtoPayload(descriptorRepeated.getFields().get(2), message.getField(descriptorRepeated.getFields().get(2)), true));

        assertTrue(result instanceof List<?>);
        assertTrue(((List<?>) result).stream().allMatch(element -> element instanceof String));
    }

    @Test
    public void shouldReturnListObjectAsItIsWhenTypeIsDoubleList() {
        double value = 1.23;
        Message message = TestMaxComputeTypeInfo.TestFieldsRepeated.newBuilder().addAllDoubleFields(Collections.singletonList(value)).build();

        Object result = primitiveProtobufMaxComputeConverter.convertPayload(new ProtoPayload(descriptorRepeated.getFields().get(3), message.getField(descriptorRepeated.getFields().get(3)), true));

        assertTrue(result instanceof List<?>);
        assertTrue(((List<?>) result).stream().allMatch(element -> element instanceof Double));
    }

    @Test
    public void shouldReturnListObjectAsItIsWhenTypeIsFloatList() {
        float value = 1.23f;
        Message message = TestMaxComputeTypeInfo.TestFieldsRepeated.newBuilder().addAllFloatFields(Collections.singletonList(value)).build();

        Object result = primitiveProtobufMaxComputeConverter.convertPayload(new ProtoPayload(descriptorRepeated.getFields().get(4), message.getField(descriptorRepeated.getFields().get(4)), true));

        assertTrue(result instanceof List<?>);
        assertTrue(((List<?>) result).stream().allMatch(element -> element instanceof Float));
    }

    @Test
    public void shouldReturnListObjectAsItIsWhenTypeIsBooleanList() {
        boolean value = true;
        Message message = TestMaxComputeTypeInfo.TestFieldsRepeated.newBuilder().addAllBoolFields(Collections.singletonList(value)).build();

        Object result = primitiveProtobufMaxComputeConverter.convertPayload(new ProtoPayload(descriptorRepeated.getFields().get(5), message.getField(descriptorRepeated.getFields().get(5)), true));

        assertTrue(result instanceof List<?>);
        assertTrue(((List<?>) result).stream().allMatch(element -> element instanceof Boolean));
    }

    @Test
    public void shouldReturnListObjectAsItIsWhenTypeIsInt64List() {
        long value = 123L;
        Message message = TestMaxComputeTypeInfo.TestFieldsRepeated.newBuilder().addAllInt64Fields(Collections.singletonList(value)).build();

        Object result = primitiveProtobufMaxComputeConverter.convertPayload(new ProtoPayload(descriptorRepeated.getFields().get(6), message.getField(descriptorRepeated.getFields().get(6)), true));

        assertTrue(result instanceof List<?>);
        assertTrue(((List<?>) result).stream().allMatch(element -> element instanceof Long));
    }

    @Test
    public void shouldReturnListObjectAsItIsWhenTypeIsUInt64List() {
        long value = 123L;
        Message message = TestMaxComputeTypeInfo.TestFieldsRepeated.newBuilder().addAllUint64Fields(Collections.singletonList(value)).build();

        Object result = primitiveProtobufMaxComputeConverter.convertPayload(new ProtoPayload(descriptorRepeated.getFields().get(7), message.getField(descriptorRepeated.getFields().get(7)), true));

        assertTrue(result instanceof List<?>);
        assertTrue(((List<?>) result).stream().allMatch(element -> element instanceof Long));
    }

    @Test
    public void shouldReturnListObjectAsItIsWhenTypeIsInt32List() {
        int value = 123;
        Message message = TestMaxComputeTypeInfo.TestFieldsRepeated.newBuilder().addAllInt32Fields(Collections.singletonList(value)).build();

        Object result = primitiveProtobufMaxComputeConverter.convertPayload(new ProtoPayload(descriptorRepeated.getFields().get(8), message.getField(descriptorRepeated.getFields().get(8)), true));

        assertTrue(result instanceof List<?>);
        assertTrue(((List<?>) result).stream().allMatch(element -> element instanceof Integer));
    }

    @Test
    public void shouldReturnListObjectAsItIsWhenTypeIsUInt32List() {
        int value = 123;
        Message message = TestMaxComputeTypeInfo.TestFieldsRepeated.newBuilder().addAllUint32Fields(Collections.singletonList(value)).build();

        Object result = primitiveProtobufMaxComputeConverter.convertPayload(new ProtoPayload(descriptorRepeated.getFields().get(9), message.getField(descriptorRepeated.getFields().get(9)), true));

        assertTrue(result instanceof List<?>);
        assertTrue(((List<?>) result).stream().allMatch(element -> element instanceof Integer));
    }

    @Test
    public void shouldReturnListObjectAsItIsWhenTypeIsFixed64List() {
        long value = 123L;
        Message message = TestMaxComputeTypeInfo.TestFieldsRepeated.newBuilder().addAllFixed64Fields(Collections.singletonList(value)).build();

        Object result = primitiveProtobufMaxComputeConverter.convertPayload(new ProtoPayload(descriptorRepeated.getFields().get(10), message.getField(descriptorRepeated.getFields().get(10)), true));

        assertTrue(result instanceof List<?>);
        assertTrue(((List<?>) result).stream().allMatch(element -> element instanceof Long));
    }

    @Test
    public void shouldReturnListObjectAsItIsWhenTypeIsFixed32List() {
        int value = 123;
        Message message = TestMaxComputeTypeInfo.TestFieldsRepeated.newBuilder().addAllFixed32Fields(Collections.singletonList(value)).build();

        Object result = primitiveProtobufMaxComputeConverter.convertPayload(new ProtoPayload(descriptorRepeated.getFields().get(11), message.getField(descriptorRepeated.getFields().get(11)), true));

        assertTrue(result instanceof List<?>);
        assertTrue(((List<?>) result).stream().allMatch(element -> element instanceof Integer));
    }

    @Test
    public void shouldReturnListObjectAsItIsWhenTypeIsSFixed32List() {
        int value = 123;
        Message message = TestMaxComputeTypeInfo.TestFieldsRepeated.newBuilder().addAllSfixed32Fields(Collections.singletonList(value)).build();

        Object result = primitiveProtobufMaxComputeConverter.convertPayload(new ProtoPayload(descriptorRepeated.getFields().get(12), message.getField(descriptorRepeated.getFields().get(12)), true));

        assertTrue(result instanceof List<?>);
        assertTrue(((List<?>) result).stream().allMatch(element -> element instanceof Integer));
    }

    @Test
    public void shouldReturnListObjectAsItIsWhenTypeIsSFixed64List() {
        long value = 123L;
        Message message = TestMaxComputeTypeInfo.TestFieldsRepeated.newBuilder().addAllSfixed64Fields(Collections.singletonList(value)).build();

        Object result = primitiveProtobufMaxComputeConverter.convertPayload(new ProtoPayload(descriptorRepeated.getFields().get(13), message.getField(descriptorRepeated.getFields().get(13)), true));

        assertTrue(result instanceof List<?>);
        assertTrue(((List<?>) result).stream().allMatch(element -> element instanceof Long));
    }

    @Test
    public void shouldReturnListObjectAsItIsWhenTypeIsSInt32List() {
        int value = 123;
        Message message = TestMaxComputeTypeInfo.TestFieldsRepeated.newBuilder().addAllSint32Fields(Collections.singletonList(value)).build();

        Object result = primitiveProtobufMaxComputeConverter.convertPayload(new ProtoPayload(descriptorRepeated.getFields().get(14), message.getField(descriptorRepeated.getFields().get(14)), true));

        assertTrue(result instanceof List<?>);
        assertTrue(((List<?>) result).stream().allMatch(element -> element instanceof Integer));
    }

    @Test
    public void shouldReturnListObjectAsItIsWhenTypeIsSInt64List() {
        long value = 123L;
        Message message = TestMaxComputeTypeInfo.TestFieldsRepeated.newBuilder().addAllSint64Fields(Collections.singletonList(value)).build();

        Object result = primitiveProtobufMaxComputeConverter.convertPayload(new ProtoPayload(descriptorRepeated.getFields().get(15), message.getField(descriptorRepeated.getFields().get(15)), true));

        assertTrue(result instanceof List<?>);
        assertTrue(((List<?>) result).stream().allMatch(element -> element instanceof Long));
    }

}
