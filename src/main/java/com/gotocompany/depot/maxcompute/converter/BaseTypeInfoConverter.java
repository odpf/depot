package com.gotocompany.depot.maxcompute.converter;

import com.aliyun.odps.type.TypeInfo;
import com.google.protobuf.Descriptors;

import java.util.ArrayList;
import java.util.List;

public class BaseTypeInfoConverter {
    private final List<TypeInfoConverter> converters;

    public BaseTypeInfoConverter() {
        converters = new ArrayList<>();
        converters.add(new PrimitiveTypeInfoConverter());
        converters.add(new DurationTypeInfoConverter());
        converters.add(new StructTypeInfoConverter());
        converters.add(new TimestampTypeInfoConverter());
        converters.add(new MessageTypeInfoConverter(converters));
        converters.sort((c1, c2) -> Integer.compare(c2.getPriority(), c1.getPriority()));
    }

    public TypeInfo convert(Descriptors.FieldDescriptor fieldDescriptor) {
        return converters.stream()
                .filter(converter -> converter.canConvert(fieldDescriptor))
                .findFirst()
                .map(converter -> converter.convert(fieldDescriptor))
                .orElseThrow(() -> new IllegalArgumentException("Unsupported type: " + fieldDescriptor.getType()));
    }
}
