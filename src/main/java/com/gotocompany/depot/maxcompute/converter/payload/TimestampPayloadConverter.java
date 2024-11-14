package com.gotocompany.depot.maxcompute.converter.payload;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.gotocompany.depot.maxcompute.converter.type.TimestampTypeInfoConverter;
import lombok.RequiredArgsConstructor;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

@RequiredArgsConstructor
public class TimestampPayloadConverter implements PayloadConverter {

    private static final String SECONDS = "seconds";
    private static final String NANOS = "nanos";

    private final TimestampTypeInfoConverter timestampTypeInfoConverter;

    @Override
    public Object convertSingular(Descriptors.FieldDescriptor fieldDescriptor, Object object) {
        Message message = (Message) object;
        long seconds = (long) message.getField(message.getDescriptorForType().findFieldByName(SECONDS));
        int nanos = (int) message.getField(message.getDescriptorForType().findFieldByName(NANOS));
        return LocalDateTime.ofEpochSecond(seconds, nanos, ZoneOffset.UTC);
    }

    @Override
    public boolean canConvert(Descriptors.FieldDescriptor fieldDescriptor) {
        return timestampTypeInfoConverter.canConvert(fieldDescriptor);
    }

}
