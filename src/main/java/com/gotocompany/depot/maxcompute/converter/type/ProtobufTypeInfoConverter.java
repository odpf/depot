package com.gotocompany.depot.maxcompute.converter.type;

import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.protobuf.Descriptors;

public interface ProtobufTypeInfoConverter {
    default TypeInfo convert(Descriptors.FieldDescriptor fieldDescriptor) {
        return wrap(fieldDescriptor, convertSingular(fieldDescriptor));
    }
    TypeInfo convertSingular(Descriptors.FieldDescriptor fieldDescriptor);
    boolean canConvert(Descriptors.FieldDescriptor fieldDescriptor);
    default TypeInfo wrap(Descriptors.FieldDescriptor fieldDescriptor, TypeInfo typeInfo) {
        return fieldDescriptor.isRepeated() ? TypeInfoFactory.getArrayTypeInfo(typeInfo) : typeInfo;
    }
}
