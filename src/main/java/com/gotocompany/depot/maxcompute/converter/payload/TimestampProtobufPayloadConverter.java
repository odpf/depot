package com.gotocompany.depot.maxcompute.converter.payload;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.exception.InvalidMessageException;
import com.gotocompany.depot.maxcompute.converter.type.TimestampProtobufTypeInfoConverter;
import lombok.RequiredArgsConstructor;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

@RequiredArgsConstructor
public class TimestampProtobufPayloadConverter implements ProtobufPayloadConverter {

    private static final String SECONDS = "seconds";
    private static final String NANOS = "nanos";

    private final TimestampProtobufTypeInfoConverter timestampTypeInfoConverter;
    private final MaxComputeSinkConfig maxComputeSinkConfig;

    @Override
    public Object convertSingular(Descriptors.FieldDescriptor fieldDescriptor, Object object) {
        Message message = (Message) object;
        long seconds = (long) message.getField(message.getDescriptorForType().findFieldByName(SECONDS));
        int nanos = (int) message.getField(message.getDescriptorForType().findFieldByName(NANOS));
        Instant instant = Instant.now();
        ZoneOffset zoneOffset = maxComputeSinkConfig.getZoneId().getRules().getOffset(instant);
        LocalDateTime localDateTime = LocalDateTime.ofEpochSecond(seconds, nanos, zoneOffset);
        validateTimestampRange(localDateTime);
        return localDateTime;
    }

    @Override
    public boolean canConvert(Descriptors.FieldDescriptor fieldDescriptor) {
        return timestampTypeInfoConverter.canConvert(fieldDescriptor);
    }

    private void validateTimestampRange(LocalDateTime localDateTime) {
        if (localDateTime.isBefore(maxComputeSinkConfig.getValidMinTimestamp()) || localDateTime.isAfter(maxComputeSinkConfig.getValidMaxTimestamp())) {
            throw new InvalidMessageException(String.format("Timestamp %s is out of allowed range range min: %s max: %s",
                    localDateTime, maxComputeSinkConfig.getValidMinTimestamp(), maxComputeSinkConfig.getValidMaxTimestamp()));
        }
    }

}
