package com.gotocompany.depot.maxcompute.converter.type;

import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Descriptors;

import java.util.Map;

import static com.google.protobuf.Descriptors.FieldDescriptor.Type.*;

public class PrimitiveProtobufTypeInfoConverter implements ProtobufTypeInfoConverter {

    private static final Map<Descriptors.FieldDescriptor.Type, TypeInfo> PROTO_TYPE_MAP;

    static {
        PROTO_TYPE_MAP = ImmutableMap.<Descriptors.FieldDescriptor.Type, TypeInfo>builder()
                .put(BYTES, TypeInfoFactory.BINARY)
                .put(STRING, TypeInfoFactory.STRING)
                .put(ENUM, TypeInfoFactory.STRING)
                .put(DOUBLE, TypeInfoFactory.DOUBLE)
                .put(FLOAT, TypeInfoFactory.FLOAT)
                .put(BOOL, TypeInfoFactory.BOOLEAN)
                .put(INT64, TypeInfoFactory.BIGINT)
                .put(UINT64, TypeInfoFactory.BIGINT)
                .put(INT32, TypeInfoFactory.INT)
                .put(UINT32, TypeInfoFactory.INT)
                .put(FIXED64, TypeInfoFactory.BIGINT)
                .put(FIXED32, TypeInfoFactory.INT)
                .put(SFIXED32, TypeInfoFactory.INT)
                .put(SFIXED64, TypeInfoFactory.BIGINT)
                .put(SINT32, TypeInfoFactory.INT)
                .put(SINT64, TypeInfoFactory.BIGINT)
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
