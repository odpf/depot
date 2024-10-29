package com.gotocompany.depot.maxcompute.converter.payload;

import com.aliyun.odps.data.SimpleStruct;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.gotocompany.depot.maxcompute.converter.type.MessageTypeInfoConverter;
import lombok.RequiredArgsConstructor;

import java.util.ArrayList;
import java.util.List;

@RequiredArgsConstructor
public class MessagePayloadConverter implements PayloadConverter {

    private final MessageTypeInfoConverter messageTypeInfoConverter;
    private final List<PayloadConverter> payloadConverters;

    @Override
    public Object convertSingular(Descriptors.FieldDescriptor fieldDescriptor, Object object) {
        Message dynamicMessage = (Message) object;
        List<Object> values = new ArrayList<>();
        fieldDescriptor.getMessageType().getFields().forEach(innerFieldDescriptor -> {
            Object mappedInnerValue = payloadConverters.stream()
                    .filter(converter -> converter.canConvert(innerFieldDescriptor))
                    .findFirst()
                    .map(converter -> converter.convert(innerFieldDescriptor, dynamicMessage.getField(innerFieldDescriptor)))
                    .orElse(null);
            values.add(mappedInnerValue);
        });
        return new SimpleStruct(messageTypeInfoConverter.convertSingular(fieldDescriptor), values);
    }

    @Override
    public boolean canConvert(Descriptors.FieldDescriptor fieldDescriptor) {
        return messageTypeInfoConverter.canConvert(fieldDescriptor);
    }

}
