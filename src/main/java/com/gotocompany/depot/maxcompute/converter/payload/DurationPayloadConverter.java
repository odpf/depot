package com.gotocompany.depot.maxcompute.converter.payload;

import com.aliyun.odps.data.SimpleStruct;
import com.aliyun.odps.type.StructTypeInfo;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.gotocompany.depot.maxcompute.converter.type.DurationTypeInfoConverter;
import lombok.RequiredArgsConstructor;

import java.util.ArrayList;
import java.util.List;

@RequiredArgsConstructor
public class DurationPayloadConverter implements PayloadConverter {

    private static final String SECONDS = "seconds";
    private static final String NANOS = "nanos";

    private final DurationTypeInfoConverter durationTypeInfoConverter;

    @Override
    public Object convertSingular(Descriptors.FieldDescriptor fieldDescriptor, Object object) {
        List<Object> values = getValues((Message) object);
        return new SimpleStruct((StructTypeInfo) durationTypeInfoConverter.convertSingular(fieldDescriptor), values);
    }

    @Override
    public boolean canConvert(Descriptors.FieldDescriptor fieldDescriptor) {
        return durationTypeInfoConverter.canConvert(fieldDescriptor);
    }

    private static List<Object> getValues(Message durationMessage) {
        List<Object> values = new ArrayList<>();
        values.add(durationMessage.getField(durationMessage.getDescriptorForType().findFieldByName(SECONDS)));
        values.add(durationMessage.getField(durationMessage.getDescriptorForType().findFieldByName(NANOS)));
        return values;
    }

}
