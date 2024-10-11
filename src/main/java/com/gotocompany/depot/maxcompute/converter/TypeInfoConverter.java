package com.gotocompany.depot.maxcompute.converter;

import com.aliyun.odps.type.TypeInfo;
import com.google.protobuf.Descriptors;

public interface TypeInfoConverter {
    TypeInfo convert(Descriptors.FieldDescriptor fieldDescriptor);
    boolean canConvert(Descriptors.FieldDescriptor fieldDescriptor);
    default int getPriority() {
        return 0;
    };
}
