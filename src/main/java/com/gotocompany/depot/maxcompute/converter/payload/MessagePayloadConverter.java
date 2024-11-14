package com.gotocompany.depot.maxcompute.converter.payload;

import com.aliyun.odps.data.SimpleStruct;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.gotocompany.depot.maxcompute.converter.type.MessageTypeInfoConverter;
import lombok.RequiredArgsConstructor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@RequiredArgsConstructor
public class MessagePayloadConverter implements PayloadConverter {

    private final MessageTypeInfoConverter messageTypeInfoConverter;
    private final List<PayloadConverter> payloadConverters;

    @Override
    public Object convertSingular(Descriptors.FieldDescriptor fieldDescriptor, Object object) {
        Message dynamicMessage = (Message) object;
        List<Object> values = new ArrayList<>();
        Map<Descriptors.FieldDescriptor, Object> payloadFields = dynamicMessage.getAllFields();
        fieldDescriptor.getMessageType().getFields().forEach(innerFieldDescriptor -> {
            if (!payloadFields.containsKey(innerFieldDescriptor)) {
                values.add(null);
                return;
            }
            Object mappedInnerValue = payloadConverters.stream()
                    .filter(converter -> converter.canConvert(innerFieldDescriptor))
                    .findFirst()
                    .map(converter -> converter.convert(innerFieldDescriptor, payloadFields.get(innerFieldDescriptor)))
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
