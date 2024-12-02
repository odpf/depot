package com.gotocompany.depot.maxcompute.converter.payload;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Timestamp;
import com.gotocompany.depot.TestMaxComputeTypeInfo;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.maxcompute.converter.type.TimestampProtobufTypeInfoConverter;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class TimestampProtobufPayloadConverterTest {

    private final TimestampProtobufTypeInfoConverter timestampTypeInfoConverter = new TimestampProtobufTypeInfoConverter();
    private final Descriptors.Descriptor descriptor = TestMaxComputeTypeInfo.TestRoot.getDescriptor();
    private final Descriptors.Descriptor repeatedDescriptor = TestMaxComputeTypeInfo.TestRootRepeated.getDescriptor();
    private TimestampProtobufPayloadConverter timestampPayloadConverter = new TimestampProtobufPayloadConverter(timestampTypeInfoConverter, Mockito.mock(MaxComputeSinkConfig.class));

    @Before
    public void setUp() {
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        Mockito.when(maxComputeSinkConfig.getZoneId()).thenReturn(ZoneId.of("UTC"));
        timestampPayloadConverter = new TimestampProtobufPayloadConverter(timestampTypeInfoConverter, maxComputeSinkConfig);
    }
    @Test
    public void shouldConvertToTimestampNtz() {
        Timestamp timestamp = Timestamp.newBuilder()
                .setSeconds(2500)
                .setNanos(100)
                .build();
        TestMaxComputeTypeInfo.TestRoot message = TestMaxComputeTypeInfo.TestRoot.newBuilder()
                .setTimestampField(timestamp)
                .build();
        LocalDateTime expectedLocalDateTime = LocalDateTime.ofEpochSecond(
                timestamp.getSeconds(), timestamp.getNanos(), java.time.ZoneOffset.UTC);

        Object result = timestampPayloadConverter.convertSingular(descriptor.getFields().get(3), message.getField(descriptor.getFields().get(3)));

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

        Object result = timestampPayloadConverter.convert(repeatedDescriptor.getFields().get(3), message.getField(repeatedDescriptor.getFields().get(3)));

        assertThat(result)
                .isInstanceOf(List.class);
        assertThat(((List<?>) result).stream().map(LocalDateTime.class::cast))
                .hasSize(2)
                .containsExactly(expectedLocalDateTime1, expectedLocalDateTime2);
    }

}
