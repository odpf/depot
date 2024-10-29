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

    @Override
    public TypeInfo convertSingular(Descriptors.FieldDescriptor fieldDescriptor) {
        List<String> fieldNames = Arrays.asList(SECONDS, NANOS);
        List<TypeInfo> typeInfos = Arrays.asList(TypeInfoFactory.BIGINT, TypeInfoFactory.INT);
        return TypeInfoFactory.getStructTypeInfo(fieldNames, typeInfos);
    }

    @Override
    public boolean canConvert(Descriptors.FieldDescriptor fieldDescriptor) {
        return fieldDescriptor.getType() == Descriptors.FieldDescriptor.Type.MESSAGE
                && fieldDescriptor.getMessageType().getFullName().equals(GOOGLE_PROTOBUF_DURATION);
    }

}
