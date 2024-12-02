package com.gotocompany.depot.maxcompute.converter.type;

import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.protobuf.Descriptors;

import java.util.Arrays;
import java.util.List;

public class DurationTypeInfoConverter implements TypeInfoConverter {

    private static final String SECONDS = "seconds";
    private static final String NANOS = "nanos";
    private static final String GOOGLE_PROTOBUF_DURATION = "google.protobuf.Duration";
    private static final List<String> FIELD_NAMES = Arrays.asList(SECONDS, NANOS);
    private static final List<TypeInfo> TYPE_INFOS = Arrays.asList(TypeInfoFactory.BIGINT, TypeInfoFactory.INT);

    @Override
    public TypeInfo convertSingular(Descriptors.FieldDescriptor fieldDescriptor) {
        return TypeInfoFactory.getStructTypeInfo(FIELD_NAMES, TYPE_INFOS);
    }

    @Override
    public boolean canConvert(Descriptors.FieldDescriptor fieldDescriptor) {
        return fieldDescriptor.getType() == Descriptors.FieldDescriptor.Type.MESSAGE
                && fieldDescriptor.getMessageType().getFullName().equals(GOOGLE_PROTOBUF_DURATION);
    }

}
