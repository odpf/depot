package com.gotocompany.depot.maxcompute.converter.payload;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.gotocompany.depot.maxcompute.converter.type.TimestampTypeInfoConverter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class TimestampPayloadConverter implements PayloadConverter {

    public static final int SECOND_TO_MILLIS_MULTIPLIER = 1000;
    private final TimestampTypeInfoConverter timestampTypeInfoConverter;

    @Override
    public Object convertSingular(Descriptors.FieldDescriptor fieldDescriptor, Object object) {
        Message message = (Message) object;
        long seconds = (long) message.getField(message.getDescriptorForType().findFieldByName("seconds"));
        int nanos  = (int) message.getField(message.getDescriptorForType().findFieldByName("nanos"));
        java.sql.Timestamp convertedTimestamp = new java.sql.Timestamp(seconds * SECOND_TO_MILLIS_MULTIPLIER);
        convertedTimestamp.setNanos(nanos);
        return convertedTimestamp;
    }

    @Override
    public boolean canConvert(Descriptors.FieldDescriptor fieldDescriptor) {
        return timestampTypeInfoConverter.canConvert(fieldDescriptor);
    }

}
