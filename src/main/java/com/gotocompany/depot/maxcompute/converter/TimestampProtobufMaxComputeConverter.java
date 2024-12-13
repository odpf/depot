package com.gotocompany.depot.maxcompute.converter;

import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.exception.InvalidMessageException;
import com.gotocompany.depot.maxcompute.model.ProtoPayload;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.TemporalAmount;

/**
 * Converts protobuf timestamp to LocalDateTime.
 * LocalDateTime is the java compatible type for TIMESTAMP_NTZ type used in MaxCompute.
 * It uses the configurable timezone to convert the epoch parsed from protobuf timestamp type.
 */
public class TimestampProtobufMaxComputeConverter implements ProtobufMaxComputeConverter {

    private static final String GOOGLE_PROTOBUF_TIMESTAMP = "google.protobuf.Timestamp";
    private static final String SECONDS = "seconds";
    private static final String NANOS = "nanos";
    private static final long DAYS_IN_YEAR = 365;

    private final TemporalAmount maxPastEventTimeDifference;
    private final TemporalAmount maxFutureEventTimeDifference;
    private final ZoneId zoneId;
    private final LocalDateTime validMinTimestamp;
    private final LocalDateTime validMaxTimestamp;
    private final boolean isTablePartitioningEnabled;
    private final String tablePartitionKey;
    private final int maxPastYearEventTimeDifference;
    private final int maxFutureYearEventTimeDifference;

    public TimestampProtobufMaxComputeConverter(MaxComputeSinkConfig maxComputeSinkConfig) {
        this.maxPastEventTimeDifference = Duration.ofDays(maxComputeSinkConfig.getMaxPastYearEventTimeDifference() * DAYS_IN_YEAR);
        this.maxFutureEventTimeDifference = Duration.ofDays(maxComputeSinkConfig.getMaxFutureYearEventTimeDifference() * DAYS_IN_YEAR);
        this.zoneId = maxComputeSinkConfig.getZoneId();
        this.validMinTimestamp = maxComputeSinkConfig.getValidMinTimestamp();
        this.validMaxTimestamp = maxComputeSinkConfig.getValidMaxTimestamp();
        this.isTablePartitioningEnabled = maxComputeSinkConfig.isTablePartitioningEnabled();
        this.tablePartitionKey = maxComputeSinkConfig.getTablePartitionKey();
        this.maxPastYearEventTimeDifference = maxComputeSinkConfig.getMaxPastYearEventTimeDifference();
        this.maxFutureYearEventTimeDifference = maxComputeSinkConfig.getMaxFutureYearEventTimeDifference();
    }

    @Override
    public TypeInfo convertSingularTypeInfo(Descriptors.FieldDescriptor fieldDescriptor) {
        return TypeInfoFactory.TIMESTAMP_NTZ;
    }

    @Override
    public boolean canConvert(Descriptors.FieldDescriptor fieldDescriptor) {
        return Descriptors.FieldDescriptor.Type.MESSAGE.equals(fieldDescriptor.getType())
                && fieldDescriptor.getMessageType().getFullName().equals(GOOGLE_PROTOBUF_TIMESTAMP);
    }

    @Override
    public Object convertSingularPayload(ProtoPayload protoPayload) {
        Message message = (Message) protoPayload.getParsedObject();
        long seconds = (long) message.getField(message.getDescriptorForType().findFieldByName(SECONDS));
        int nanos = (int) message.getField(message.getDescriptorForType().findFieldByName(NANOS));
        Instant instant = Instant.now();
        ZoneOffset zoneOffset = zoneId.getRules().getOffset(instant);
        LocalDateTime localDateTime = LocalDateTime.ofEpochSecond(seconds, nanos, zoneOffset);
        validateTimestampRange(localDateTime);
        validateTimestampPartitionKey(protoPayload.getFieldDescriptor().getName(), localDateTime, protoPayload.isRootLevel());
        return localDateTime;
    }

    private void validateTimestampRange(LocalDateTime localDateTime) {
        if (localDateTime.isBefore(validMinTimestamp) || localDateTime.isAfter(validMaxTimestamp)) {
            throw new InvalidMessageException(String.format("Timestamp %s is out of allowed range range min: %s max: %s",
                    localDateTime, validMinTimestamp, validMaxTimestamp));
        }
    }

    private void validateTimestampPartitionKey(String fieldName, LocalDateTime eventTime, boolean isRootLevel) {
        if (!isTablePartitioningEnabled) {
            return;
        }
        if (!isRootLevel) {
            return;
        }
        if (fieldName.equals(tablePartitionKey)) {
            Instant now = Instant.now();
            Instant eventTimeInstant = eventTime.toInstant(zoneId.getRules().getOffset(now));

            if (now.minus(maxPastEventTimeDifference).isAfter(eventTimeInstant)) {
                throw new InvalidMessageException(String.format("Timestamp is in the past, you can only stream data within %d year(s) in the past", maxPastYearEventTimeDifference));
            }
            if (now.plus(maxFutureEventTimeDifference).isBefore(eventTimeInstant)) {
                throw new InvalidMessageException(String.format("Timestamp is in the future, you can only stream data within %d year(s) in the future", maxFutureYearEventTimeDifference));
            }
        }
    }

}
