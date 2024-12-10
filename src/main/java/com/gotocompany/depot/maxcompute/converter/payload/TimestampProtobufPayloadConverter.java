package com.gotocompany.depot.maxcompute.converter.payload;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.exception.InvalidMessageException;
import com.gotocompany.depot.maxcompute.converter.type.TimestampProtobufTypeInfoConverter;
import com.gotocompany.depot.maxcompute.model.ProtoPayload;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.TemporalAmount;

public class TimestampProtobufPayloadConverter implements ProtobufPayloadConverter {

    private static final String SECONDS = "seconds";
    private static final String NANOS = "nanos";
    private static final long DAYS_IN_YEAR = 365;

    private final TimestampProtobufTypeInfoConverter timestampTypeInfoConverter;
    private final MaxComputeSinkConfig maxComputeSinkConfig;
    private final TemporalAmount maxPastEventTimeDifference;
    private final TemporalAmount maxFutureEventTimeDifference;

    public TimestampProtobufPayloadConverter(TimestampProtobufTypeInfoConverter timestampTypeInfoConverter, MaxComputeSinkConfig maxComputeSinkConfig) {
        this.timestampTypeInfoConverter = timestampTypeInfoConverter;
        this.maxComputeSinkConfig = maxComputeSinkConfig;
        this.maxPastEventTimeDifference = Duration.ofDays(maxComputeSinkConfig.getMaxPastYearEventTimeDifference() * DAYS_IN_YEAR);
        this.maxFutureEventTimeDifference = Duration.ofDays(maxComputeSinkConfig.getMaxFutureYearEventTimeDifference() * DAYS_IN_YEAR);
    }

    @Override
    public Object convertSingular(ProtoPayload protoPayload) {
        Message message = (Message) protoPayload.getParsedObject();
        long seconds = (long) message.getField(message.getDescriptorForType().findFieldByName(SECONDS));
        int nanos = (int) message.getField(message.getDescriptorForType().findFieldByName(NANOS));
        Instant instant = Instant.now();
        ZoneOffset zoneOffset = maxComputeSinkConfig.getZoneId().getRules().getOffset(instant);
        LocalDateTime localDateTime = LocalDateTime.ofEpochSecond(seconds, nanos, zoneOffset);
        validateTimestampRange(localDateTime);
        validateTimestampPartitionKey(protoPayload.getFieldDescriptor().getName(), localDateTime, protoPayload.isRootLevel());
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

    private void validateTimestampPartitionKey(String fieldName, LocalDateTime eventTime, boolean isRootLevel) {
        if (!maxComputeSinkConfig.isTablePartitioningEnabled()) {
            return;
        }
        if (!isRootLevel) {
            return;
        }
        if (fieldName.equals(maxComputeSinkConfig.getTablePartitionKey())) {
            Instant now = Instant.now();
            Instant eventTimeInstant = eventTime.toInstant(maxComputeSinkConfig.getZoneId().getRules().getOffset(now));

            if (now.minus(maxPastEventTimeDifference).isAfter(eventTimeInstant)) {
                throw new InvalidMessageException(String.format("Timestamp is in the past, you can only stream data within %d year(s) in the past", maxComputeSinkConfig.getMaxPastYearEventTimeDifference()));
            }
            if (now.plus(maxFutureEventTimeDifference).isBefore(eventTimeInstant)) {
                throw new InvalidMessageException(String.format("Timestamp is in the future, you can only stream data within %d year(s) in the future", maxComputeSinkConfig.getMaxFutureYearEventTimeDifference()));
            }
        }
    }

}
