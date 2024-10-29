package com.gotocompany.depot.maxcompute.converter.payload;

import com.google.protobuf.Descriptors;

import java.util.List;
import java.util.stream.Collectors;

public interface PayloadConverter {

    default Object convert(Descriptors.FieldDescriptor fieldDescriptor, Object object) {
        if (!fieldDescriptor.isRepeated()) {
            return convertSingular(fieldDescriptor, object);
        }
        return ((List<?>) object).stream()
                .map(o -> convertSingular(fieldDescriptor, o))
                .collect(Collectors.toList());
    }

    Object convertSingular(Descriptors.FieldDescriptor fieldDescriptor, Object object);

    boolean canConvert(Descriptors.FieldDescriptor fieldDescriptor);
}
