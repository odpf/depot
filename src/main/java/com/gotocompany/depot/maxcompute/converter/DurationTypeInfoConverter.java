package com.gotocompany.depot.maxcompute.converter;

import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.protobuf.Descriptors;

import java.util.ArrayList;
import java.util.List;

public class DurationTypeInfoConverter implements TypeInfoConverter {

    private static final String SECONDS = "seconds";
    private static final String NANOS = "nanos";

    @Override
    public TypeInfo convert(Descriptors.FieldDescriptor fieldDescriptor) {
        List<String> fieldNames = new ArrayList<>();
        fieldNames.add(SECONDS);
        fieldNames.add(NANOS);
        List<TypeInfo> typeInfos = new ArrayList<>();
        typeInfos.add(TypeInfoFactory.BIGINT);
        typeInfos.add(TypeInfoFactory.INT);

        return TypeInfoFactory.getStructTypeInfo(fieldNames, typeInfos);
    }

    @Override
    public boolean canConvert(Descriptors.FieldDescriptor fieldDescriptor) {
        return fieldDescriptor.getType() == Descriptors.FieldDescriptor.Type.MESSAGE
                && fieldDescriptor.getMessageType().getFullName().equals("google.protobuf.Duration");
    }

}
