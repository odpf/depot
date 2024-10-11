package com.gotocompany.depot.maxcompute.converter;

import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.protobuf.Descriptors;

import java.util.HashMap;
import java.util.Map;

public class PrimitiveTypeInfoConverter implements TypeInfoConverter {

    private static final Map<Descriptors.FieldDescriptor.Type, TypeInfo> PROTO_TYPE_MAP;
    
    static {
        PROTO_TYPE_MAP = new HashMap<>();
        PROTO_TYPE_MAP.put(Descriptors.FieldDescriptor.Type.BYTES, TypeInfoFactory.BINARY);
        PROTO_TYPE_MAP.put(Descriptors.FieldDescriptor.Type.STRING, TypeInfoFactory.STRING);
        PROTO_TYPE_MAP.put(Descriptors.FieldDescriptor.Type.ENUM, TypeInfoFactory.STRING);
        PROTO_TYPE_MAP.put(Descriptors.FieldDescriptor.Type.DOUBLE, TypeInfoFactory.DOUBLE);
        PROTO_TYPE_MAP.put(Descriptors.FieldDescriptor.Type.FLOAT, TypeInfoFactory.FLOAT);
        PROTO_TYPE_MAP.put(Descriptors.FieldDescriptor.Type.BOOL, TypeInfoFactory.BOOLEAN);
        PROTO_TYPE_MAP.put(Descriptors.FieldDescriptor.Type.INT64, TypeInfoFactory.BIGINT);
        PROTO_TYPE_MAP.put(Descriptors.FieldDescriptor.Type.UINT64, TypeInfoFactory.BIGINT);
        PROTO_TYPE_MAP.put(Descriptors.FieldDescriptor.Type.INT32, TypeInfoFactory.INT);
        PROTO_TYPE_MAP.put(Descriptors.FieldDescriptor.Type.UINT32, TypeInfoFactory.INT);
        PROTO_TYPE_MAP.put(Descriptors.FieldDescriptor.Type.FIXED64, TypeInfoFactory.BIGINT);
        PROTO_TYPE_MAP.put(Descriptors.FieldDescriptor.Type.FIXED32, TypeInfoFactory.INT);
        PROTO_TYPE_MAP.put(Descriptors.FieldDescriptor.Type.SFIXED32, TypeInfoFactory.INT);
        PROTO_TYPE_MAP.put(Descriptors.FieldDescriptor.Type.SFIXED64, TypeInfoFactory.BIGINT);
        PROTO_TYPE_MAP.put(Descriptors.FieldDescriptor.Type.SINT32, TypeInfoFactory.INT);
        PROTO_TYPE_MAP.put(Descriptors.FieldDescriptor.Type.SINT64, TypeInfoFactory.BIGINT);
    }

    @Override
    public TypeInfo convert(Descriptors.FieldDescriptor fieldDescriptor) {
        TypeInfo typeInfo = PROTO_TYPE_MAP.get(fieldDescriptor.getType());
        return fieldDescriptor.isRepeated() ? TypeInfoFactory.getArrayTypeInfo(typeInfo) : typeInfo;
    }

    @Override
    public boolean canConvert(Descriptors.FieldDescriptor fieldDescriptor) {
        return PROTO_TYPE_MAP.containsKey(fieldDescriptor.getType());
    }
    
}
