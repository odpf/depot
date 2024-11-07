package com.gotocompany.depot.maxcompute.converter.payload;

import com.aliyun.odps.data.Binary;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.gotocompany.depot.maxcompute.converter.type.PrimitiveTypeInfoConverter;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public class PrimitivePayloadConverter implements PayloadConverter {

    private final Map<Descriptors.FieldDescriptor.Type, Function<Object, Object>> mappers;

    private final PrimitiveTypeInfoConverter primitiveTypeInfoConverter;

    public PrimitivePayloadConverter(PrimitiveTypeInfoConverter primitiveTypeInfoConverter) {
        this.primitiveTypeInfoConverter = primitiveTypeInfoConverter;
        this.mappers = new HashMap<>();
        this.mappers.put(Descriptors.FieldDescriptor.Type.BYTES, object -> new Binary(((ByteString) object).toByteArray()));
        this.mappers.put(Descriptors.FieldDescriptor.Type.ENUM, Object::toString);
    }

    @Override
    public Object convertSingular(Descriptors.FieldDescriptor fieldDescriptor, Object object) {
        return mappers.getOrDefault(fieldDescriptor.getType(), Function.identity()).apply(object);
    }

    @Override
    public boolean canConvert(Descriptors.FieldDescriptor fieldDescriptor) {
        return primitiveTypeInfoConverter.canConvert(fieldDescriptor);
    }
}
