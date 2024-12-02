package com.gotocompany.depot.maxcompute.converter.payload;

import com.aliyun.odps.data.SimpleStruct;
import com.aliyun.odps.type.StructTypeInfo;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.gotocompany.depot.maxcompute.converter.type.DurationProtobufTypeInfoConverter;
import com.gotocompany.depot.maxcompute.model.ProtoPayload;
import lombok.RequiredArgsConstructor;

import java.util.ArrayList;
import java.util.List;

@RequiredArgsConstructor
public class DurationProtobufPayloadConverter implements ProtobufPayloadConverter {

    private static final String SECONDS = "seconds";
    private static final String NANOS = "nanos";

    private final DurationProtobufTypeInfoConverter durationTypeInfoConverter;

    @Override
    public Object convertSingular(ProtoPayload protoPayload) {
        List<Object> values = getValues((Message) protoPayload.getObject());
        return new SimpleStruct((StructTypeInfo) durationTypeInfoConverter.convertSingular(protoPayload.getFieldDescriptor()), values);
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
