package com.gotocompany.depot.maxcompute.converter.type;

import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Descriptors;

import java.util.Map;

public class PrimitiveTypeInfoConverter implements TypeInfoConverter {

    private static final Map<Descriptors.FieldDescriptor.Type, TypeInfo> PROTO_TYPE_MAP;

    static {
        PROTO_TYPE_MAP = ImmutableMap.<Descriptors.FieldDescriptor.Type, TypeInfo>builder()
                .put(Descriptors.FieldDescriptor.Type.BYTES, TypeInfoFactory.BINARY)
                .put(Descriptors.FieldDescriptor.Type.STRING, TypeInfoFactory.STRING)
                .put(Descriptors.FieldDescriptor.Type.ENUM, TypeInfoFactory.STRING)
                .put(Descriptors.FieldDescriptor.Type.DOUBLE, TypeInfoFactory.DOUBLE)
                .put(Descriptors.FieldDescriptor.Type.FLOAT, TypeInfoFactory.FLOAT)
                .put(Descriptors.FieldDescriptor.Type.BOOL, TypeInfoFactory.BOOLEAN)
                .put(Descriptors.FieldDescriptor.Type.INT64, TypeInfoFactory.BIGINT)
                .put(Descriptors.FieldDescriptor.Type.UINT64, TypeInfoFactory.BIGINT)
                .put(Descriptors.FieldDescriptor.Type.INT32, TypeInfoFactory.INT)
                .put(Descriptors.FieldDescriptor.Type.UINT32, TypeInfoFactory.INT)
                .put(Descriptors.FieldDescriptor.Type.FIXED64, TypeInfoFactory.BIGINT)
                .put(Descriptors.FieldDescriptor.Type.FIXED32, TypeInfoFactory.INT)
                .put(Descriptors.FieldDescriptor.Type.SFIXED32, TypeInfoFactory.INT)
                .put(Descriptors.FieldDescriptor.Type.SFIXED64, TypeInfoFactory.BIGINT)
                .put(Descriptors.FieldDescriptor.Type.SINT32, TypeInfoFactory.INT)
                .put(Descriptors.FieldDescriptor.Type.SINT64, TypeInfoFactory.BIGINT)
                .build();
    }

    @Override
    public TypeInfo convertSingular(Descriptors.FieldDescriptor fieldDescriptor) {
        return PROTO_TYPE_MAP.get(fieldDescriptor.getType());
    }

    @Override
    public boolean canConvert(Descriptors.FieldDescriptor fieldDescriptor) {
        return PROTO_TYPE_MAP.containsKey(fieldDescriptor.getType());
    }
}
