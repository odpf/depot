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
        this.mappers.put(Descriptors.FieldDescriptor.Type.BYTES, object -> handleBytes((ByteString) object));
        this.mappers.put(Descriptors.FieldDescriptor.Type.ENUM, Object::toString);
        this.mappers.put(Descriptors.FieldDescriptor.Type.FLOAT, object -> handleFloat((float) object));
        this.mappers.put(Descriptors.FieldDescriptor.Type.DOUBLE, object -> handleDouble((double) object));
    }

    @Override
    public Object convertSingular(ProtoPayload protoPayload) {
        return mappers.getOrDefault(protoPayload.getFieldDescriptor().getType(), Function.identity()).apply(protoPayload.getParsedObject());
    }

    @Override
    public boolean canConvert(Descriptors.FieldDescriptor fieldDescriptor) {
        return primitiveTypeInfoConverter.canConvert(fieldDescriptor);
    }

    private static double handleDouble(double value) {
        if (!Double.isFinite(value)) {
            throw new InvalidMessageException("Invalid float value: " + value);
        }
        return value;
    }

    private static float handleFloat(float value) {
        if (!Float.isFinite(value)) {
            throw new InvalidMessageException("Invalid float value: " + value);
        }
        return value;
    }

    private static Binary handleBytes(ByteString object) {
        return new Binary(object.toByteArray());
    }

}
