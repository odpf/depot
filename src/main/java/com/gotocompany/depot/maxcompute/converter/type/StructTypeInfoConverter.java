package com.gotocompany.depot.maxcompute.converter.type;

import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.protobuf.Descriptors;

public class StructTypeInfoConverter implements TypeInfoConverter {

    private static final String GOOGLE_PROTOBUF_STRUCT = "google.protobuf.Struct";

    @Override
    public TypeInfo convertSingular(Descriptors.FieldDescriptor fieldDescriptor) {
        return TypeInfoFactory.STRING;
    }

    @Override
    public boolean canConvert(Descriptors.FieldDescriptor fieldDescriptor) {
        return fieldDescriptor.getType() == Descriptors.FieldDescriptor.Type.MESSAGE
                && fieldDescriptor.getMessageType().getFullName().equals(GOOGLE_PROTOBUF_STRUCT);
    }

}
