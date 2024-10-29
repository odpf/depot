package com.gotocompany.depot.maxcompute.converter.payload;

import com.aliyun.odps.data.SimpleStruct;
import com.aliyun.odps.type.StructTypeInfo;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Duration;
import com.gotocompany.depot.maxcompute.converter.type.DurationTypeInfoConverter;
import lombok.RequiredArgsConstructor;

import java.util.ArrayList;
import java.util.List;

@RequiredArgsConstructor
public class DurationPayloadConverter implements PayloadConverter {

    private final DurationTypeInfoConverter durationTypeInfoConverter;

    @Override
    public Object convertSingular(Descriptors.FieldDescriptor fieldDescriptor, Object object) {
        List<Object> values = getValues((Duration) object);

        return new SimpleStruct((StructTypeInfo) durationTypeInfoConverter.convertSingular(fieldDescriptor), values);
    }

    @Override
    public boolean canConvert(Descriptors.FieldDescriptor fieldDescriptor) {
        return durationTypeInfoConverter.canConvert(fieldDescriptor);
    }

    private static List<Object> getValues(Duration object) {
        List<Object> values = new ArrayList<>();
        values.add(object.getSeconds());
        values.add(object.getNanos());
        return values;
    }

}
