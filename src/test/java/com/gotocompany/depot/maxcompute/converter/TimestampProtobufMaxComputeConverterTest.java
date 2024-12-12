package com.gotocompany.depot.maxcompute.converter;

import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Timestamp;
import com.gotocompany.depot.TestMaxComputeTypeInfo;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.exception.InvalidMessageException;
import com.gotocompany.depot.maxcompute.model.ProtoPayload;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

public class TimestampProtobufMaxComputeConverterTest {

    private static final int TIMESTAMP_INDEX = 3;
    private final Descriptors.Descriptor descriptor = TestMaxComputeTypeInfo.TestRoot.getDescriptor();
    private final Descriptors.Descriptor repeatedDescriptor = TestMaxComputeTypeInfo.TestRootRepeated.getDescriptor();

    private TimestampProtobufMaxComputeConverter timestampProtobufMaxComputeConverter;

    @Before
    public void setUp() {
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        when(maxComputeSinkConfig.getZoneId()).thenReturn(ZoneId.of("UTC"));
        when(maxComputeSinkConfig.getValidMinTimestamp()).thenReturn(LocalDateTime.parse("1970-01-01T00:00:01", DateTimeFormatter.ISO_DATE_TIME));
        when(maxComputeSinkConfig.getValidMaxTimestamp()).thenReturn(LocalDateTime.parse("9999-01-01T23:59:59", DateTimeFormatter.ISO_DATE_TIME));
        when(maxComputeSinkConfig.isTablePartitioningEnabled()).thenReturn(true);
        when(maxComputeSinkConfig.getTablePartitionKey()).thenReturn("timestamp_field");
        when(maxComputeSinkConfig.getMaxPastYearEventTimeDifference()).thenReturn(999);
        when(maxComputeSinkConfig.getMaxFutureYearEventTimeDifference()).thenReturn(999);
        timestampProtobufMaxComputeConverter = new TimestampProtobufMaxComputeConverter(maxComputeSinkConfig);
    }

    @Test
    public void shouldConvertToTimestampNtz() {
        TypeInfo typeInfo = timestampProtobufMaxComputeConverter.convertTypeInfo(descriptor.getFields().get(TIMESTAMP_INDEX));

        assertEquals(TypeInfoFactory.TIMESTAMP_NTZ, typeInfo);
    }

    @Test
    public void shouldReturnTrueWhenCanConvertIsCalledWithTimestampFieldDescriptor() {
        assertTrue(timestampProtobufMaxComputeConverter.canConvert(descriptor.getFields().get(TIMESTAMP_INDEX)));
    }

    @Test
    public void shouldReturnFalseWhenCanConvertIsCalledWithNonTimestampFieldDescriptor() {
        assertFalse(timestampProtobufMaxComputeConverter.canConvert(descriptor.getFields().get(0)));
        assertFalse(timestampProtobufMaxComputeConverter.canConvert(descriptor.getFields().get(1)));
    }

    @Test
    public void shouldConvertPayloadToTimestampNtz() {
        Timestamp timestamp = Timestamp.newBuilder()
                .setSeconds(2500)
                .setNanos(100)
                .build();
        TestMaxComputeTypeInfo.TestRoot message = TestMaxComputeTypeInfo.TestRoot.newBuilder()
                .setTimestampField(timestamp)
                .build();
        LocalDateTime expectedLocalDateTime = LocalDateTime.ofEpochSecond(
                timestamp.getSeconds(), timestamp.getNanos(), java.time.ZoneOffset.UTC);

        Object result = timestampProtobufMaxComputeConverter.convertSingularPayload(new ProtoPayload(descriptor.getFields().get(3), message.getField(descriptor.getFields().get(3)), true));

        assertThat(result)
                .isEqualTo(expectedLocalDateTime);
    }

    @Test
    public void shouldConvertRepeatedTimestampPayloadToTimestampList() {
        Timestamp timestamp1 = Timestamp.newBuilder()
                .setSeconds(2500)
                .setNanos(100)
                .build();
        Timestamp timestamp2 = Timestamp.newBuilder()
                .setSeconds(3600)
                .setNanos(200)
                .build();
        TestMaxComputeTypeInfo.TestRootRepeated message = TestMaxComputeTypeInfo.TestRootRepeated.newBuilder()
                .addAllTimestampFields(Arrays.asList(timestamp1, timestamp2))
                .build();
        LocalDateTime expectedLocalDateTime1 = LocalDateTime.ofEpochSecond(
                timestamp1.getSeconds(), timestamp1.getNanos(), java.time.ZoneOffset.UTC);
        LocalDateTime expectedLocalDateTime2 = LocalDateTime.ofEpochSecond(
                timestamp2.getSeconds(), timestamp2.getNanos(), java.time.ZoneOffset.UTC);

        Object result = timestampProtobufMaxComputeConverter.convertPayload(new ProtoPayload(repeatedDescriptor.getFields().get(3), message.getField(repeatedDescriptor.getFields().get(3)), true));

        assertThat(result)
                .isInstanceOf(List.class);
        assertThat(((List<?>) result).stream().map(LocalDateTime.class::cast))
                .hasSize(2)
                .containsExactly(expectedLocalDateTime1, expectedLocalDateTime2);
    }

    @Test(expected = InvalidMessageException.class)
    public void shouldThrowInvalidMessageExceptionWhenDateIsOutOfMinValidRange() {
        Timestamp timestamp = Timestamp.newBuilder()
                .setSeconds(0)
                .setNanos(0)
                .build();
        TestMaxComputeTypeInfo.TestRoot message = TestMaxComputeTypeInfo.TestRoot.newBuilder()
                .setTimestampField(timestamp)
                .build();
        LocalDateTime expectedLocalDateTime = LocalDateTime.ofEpochSecond(
                timestamp.getSeconds(), timestamp.getNanos(), java.time.ZoneOffset.UTC);

        Object result = timestampProtobufMaxComputeConverter.convertSingularPayload(new ProtoPayload(descriptor.getFields().get(3), message.getField(descriptor.getFields().get(3)), true));

        assertThat(result)
                .isEqualTo(expectedLocalDateTime);
    }

    @Test(expected = InvalidMessageException.class)
    public void shouldThrowInvalidMessageExceptionWhenDateIsOutOfMaxValidRange() {
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        when(maxComputeSinkConfig.getZoneId()).thenReturn(ZoneId.of("UTC"));
        when(maxComputeSinkConfig.getValidMinTimestamp()).thenReturn(LocalDateTime.parse("1970-01-01T00:00:00", DateTimeFormatter.ISO_DATE_TIME));
        when(maxComputeSinkConfig.getValidMaxTimestamp()).thenReturn(LocalDateTime.parse("1970-01-01T23:59:59", DateTimeFormatter.ISO_DATE_TIME));
        timestampProtobufMaxComputeConverter = new TimestampProtobufMaxComputeConverter(maxComputeSinkConfig);

        Timestamp timestamp = Timestamp.newBuilder()
                .setSeconds(3600 * 48)
                .setNanos(0)
                .build();
        TestMaxComputeTypeInfo.TestRoot message = TestMaxComputeTypeInfo.TestRoot.newBuilder()
                .setTimestampField(timestamp)
                .build();
        LocalDateTime expectedLocalDateTime = LocalDateTime.ofEpochSecond(
                timestamp.getSeconds(), timestamp.getNanos(), java.time.ZoneOffset.UTC);

        Object result = timestampProtobufMaxComputeConverter.convertSingularPayload(new ProtoPayload(descriptor.getFields().get(3), message.getField(descriptor.getFields().get(3)), true));

        assertThat(result).isEqualTo(expectedLocalDateTime);
    }

    @Test(expected = InvalidMessageException.class)
    public void shouldThrowInvalidMessageExceptionWhenTimeDifferenceExceedsMaxPastDuration() {
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        when(maxComputeSinkConfig.getZoneId()).thenReturn(ZoneId.of("UTC"));
        when(maxComputeSinkConfig.getValidMinTimestamp()).thenReturn(LocalDateTime.parse("1970-01-01T00:00:00", DateTimeFormatter.ISO_DATE_TIME));
        when(maxComputeSinkConfig.getValidMaxTimestamp()).thenReturn(LocalDateTime.parse("9999-01-01T23:59:59", DateTimeFormatter.ISO_DATE_TIME));
        when(maxComputeSinkConfig.isTablePartitioningEnabled()).thenReturn(true);
        when(maxComputeSinkConfig.getTablePartitionKey()).thenReturn("timestamp_field");
        when(maxComputeSinkConfig.getMaxPastYearEventTimeDifference()).thenReturn(5);
        when(maxComputeSinkConfig.getMaxPastYearEventTimeDifference()).thenReturn(5);
        timestampProtobufMaxComputeConverter = new TimestampProtobufMaxComputeConverter(maxComputeSinkConfig);
        Timestamp timestamp = Timestamp.newBuilder()
                .setSeconds(3600)
                .setNanos(0)
                .build();
        TestMaxComputeTypeInfo.TestRoot message = TestMaxComputeTypeInfo.TestRoot.newBuilder()
                .setTimestampField(timestamp)
                .build();

        timestampProtobufMaxComputeConverter.convertSingularPayload(new ProtoPayload(descriptor.getFields().get(3), message.getField(descriptor.getFields().get(3)), true));
    }

    @Test(expected = InvalidMessageException.class)
    public void shouldThrowInvalidMessageExceptionWhenTimeDifferenceExceedsMaxFutureDuration() {
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        when(maxComputeSinkConfig.getZoneId()).thenReturn(ZoneId.of("UTC"));
        when(maxComputeSinkConfig.getValidMinTimestamp()).thenReturn(LocalDateTime.parse("1970-01-01T00:00:00", DateTimeFormatter.ISO_DATE_TIME));
        when(maxComputeSinkConfig.getValidMaxTimestamp()).thenReturn(LocalDateTime.parse("9999-01-01T23:59:59", DateTimeFormatter.ISO_DATE_TIME));
        when(maxComputeSinkConfig.isTablePartitioningEnabled()).thenReturn(true);
        when(maxComputeSinkConfig.getTablePartitionKey()).thenReturn("timestamp_field");
        when(maxComputeSinkConfig.getMaxPastYearEventTimeDifference()).thenReturn(5);
        when(maxComputeSinkConfig.getMaxFutureYearEventTimeDifference()).thenReturn(1);
        timestampProtobufMaxComputeConverter = new TimestampProtobufMaxComputeConverter(maxComputeSinkConfig);
        Timestamp timestamp = Timestamp.newBuilder()
                .setSeconds(System.currentTimeMillis() / 1000 + Duration.ofDays(365 * 6).toMinutes() * 60)
                .setNanos(0)
                .build();
        TestMaxComputeTypeInfo.TestRoot message = TestMaxComputeTypeInfo.TestRoot.newBuilder()
                .setTimestampField(timestamp)
                .build();

        timestampProtobufMaxComputeConverter.convertSingularPayload(new ProtoPayload(descriptor.getFields().get(3), message.getField(descriptor.getFields().get(3)), true));
    }

    @Test
    public void shouldSkipDifferenceValidationWhenPartitionDisabled() {
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        when(maxComputeSinkConfig.getZoneId()).thenReturn(ZoneId.of("UTC"));
        when(maxComputeSinkConfig.getValidMinTimestamp()).thenReturn(LocalDateTime.parse("1970-01-01T00:00:00", DateTimeFormatter.ISO_DATE_TIME));
        when(maxComputeSinkConfig.getValidMaxTimestamp()).thenReturn(LocalDateTime.parse("9999-01-01T23:59:59", DateTimeFormatter.ISO_DATE_TIME));
        when(maxComputeSinkConfig.isTablePartitioningEnabled()).thenReturn(false);
        when(maxComputeSinkConfig.getTablePartitionKey()).thenReturn("timestamp_field");
        when(maxComputeSinkConfig.getMaxPastYearEventTimeDifference()).thenReturn(5);
        when(maxComputeSinkConfig.getMaxFutureYearEventTimeDifference()).thenReturn(1);
        timestampProtobufMaxComputeConverter = new TimestampProtobufMaxComputeConverter(maxComputeSinkConfig);
        Timestamp timestamp = Timestamp.newBuilder()
                .setSeconds(System.currentTimeMillis() / 1000 + Duration.ofDays(365 * 6).toMinutes() * 60)
                .setNanos(0)
                .build();
        TestMaxComputeTypeInfo.TestRoot message = TestMaxComputeTypeInfo.TestRoot.newBuilder()
                .setTimestampField(timestamp)
                .build();
        LocalDateTime expectedLocalDateTime = LocalDateTime.ofEpochSecond(
                timestamp.getSeconds(), timestamp.getNanos(), java.time.ZoneOffset.UTC);

        LocalDateTime result = (LocalDateTime) timestampProtobufMaxComputeConverter.convertSingularPayload(
                new ProtoPayload(descriptor.getFields().get(3), message.getField(descriptor.getFields().get(3)), true));

        assertThat(result)
                .isEqualTo(expectedLocalDateTime);
    }

    @Test
    public void shouldSkipDifferenceValidationWhenIsNotRootLevel() {
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        when(maxComputeSinkConfig.getZoneId()).thenReturn(ZoneId.of("UTC"));
        when(maxComputeSinkConfig.getValidMinTimestamp()).thenReturn(LocalDateTime.parse("1970-01-01T00:00:00", DateTimeFormatter.ISO_DATE_TIME));
        when(maxComputeSinkConfig.getValidMaxTimestamp()).thenReturn(LocalDateTime.parse("9999-01-01T23:59:59", DateTimeFormatter.ISO_DATE_TIME));
        when(maxComputeSinkConfig.isTablePartitioningEnabled()).thenReturn(true);
        when(maxComputeSinkConfig.getTablePartitionKey()).thenReturn("timestamp_field");
        when(maxComputeSinkConfig.getMaxPastYearEventTimeDifference()).thenReturn(5);
        when(maxComputeSinkConfig.getMaxFutureYearEventTimeDifference()).thenReturn(1);
        timestampProtobufMaxComputeConverter = new TimestampProtobufMaxComputeConverter(maxComputeSinkConfig);
        Timestamp timestamp = Timestamp.newBuilder()
                .setSeconds(System.currentTimeMillis() / 1000 + Duration.ofDays(365 * 6).toMinutes() * 60)
                .setNanos(0)
                .build();
        TestMaxComputeTypeInfo.TestRoot message = TestMaxComputeTypeInfo.TestRoot.newBuilder()
                .setTimestampField(timestamp)
                .build();
        LocalDateTime expectedLocalDateTime = LocalDateTime.ofEpochSecond(
                timestamp.getSeconds(), timestamp.getNanos(), java.time.ZoneOffset.UTC);

        LocalDateTime result = (LocalDateTime) timestampProtobufMaxComputeConverter.convertSingularPayload(new ProtoPayload(descriptor.getFields().get(3),
                message.getField(descriptor.getFields().get(3)), false));

        assertThat(result)
                .isEqualTo(expectedLocalDateTime);
    }

}
