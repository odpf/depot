package com.gotocompany.depot.maxcompute.converter.payload;

import com.aliyun.odps.data.Binary;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.gotocompany.depot.exception.InvalidMessageException;
import com.gotocompany.depot.maxcompute.converter.type.PrimitiveProtobufTypeInfoConverter;
import com.gotocompany.depot.maxcompute.model.ProtoPayload;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public class PrimitiveProtobufPayloadConverter implements ProtobufPayloadConverter {

    private final Map<Descriptors.FieldDescriptor.Type, Function<Object, Object>> mappers;

    private final PrimitiveProtobufTypeInfoConverter primitiveTypeInfoConverter;

    public PrimitiveProtobufPayloadConverter(PrimitiveProtobufTypeInfoConverter primitiveTypeInfoConverter) {
        this.primitiveTypeInfoConverter = primitiveTypeInfoConverter;
        this.mappers = new HashMap<>();
        this.mappers.put(Descriptors.FieldDescriptor.Type.BYTES, object -> new Binary(((ByteString) object).toByteArray()));
        this.mappers.put(Descriptors.FieldDescriptor.Type.ENUM, Object::toString);
        this.mappers.put(Descriptors.FieldDescriptor.Type.FLOAT, object -> {
            float value = (float) object;
            if (!Float.isFinite(value)) {
                throw new InvalidMessageException("Invalid float value: " + value);
            }
            return value;
        });
        this.mappers.put(Descriptors.FieldDescriptor.Type.DOUBLE, object -> {
            double value = (double) object;
            if (!Double.isFinite(value)) {
                throw new InvalidMessageException("Invalid float value: " + value);
            }
            return value;
        });
    }

    @Override
    public Object convertSingular(ProtoPayload protoPayload) {
        return mappers.getOrDefault(protoPayload.getFieldDescriptor().getType(), Function.identity()).apply(protoPayload.getObject());
    }

    @Override
    public boolean canConvert(Descriptors.FieldDescriptor fieldDescriptor) {
        return primitiveTypeInfoConverter.canConvert(fieldDescriptor);
    }

}
