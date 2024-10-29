package com.gotocompany.depot.maxcompute.converter.payload;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Timestamp;
import com.gotocompany.depot.TestMaxComputeTypeInfo;
import com.gotocompany.depot.maxcompute.converter.type.TimestampTypeInfoConverter;
import org.assertj.core.api.Assertions;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class TimestampPayloadConverterTest {

    private final TimestampTypeInfoConverter timestampTypeInfoConverter = new TimestampTypeInfoConverter();
    private final TimestampPayloadConverter timestampPayloadConverter = new TimestampPayloadConverter(timestampTypeInfoConverter);
    private final Descriptors.Descriptor descriptor = TestMaxComputeTypeInfo.TestRoot.getDescriptor();
    private final Descriptors.Descriptor repeatedDescriptor = TestMaxComputeTypeInfo.TestRootRepeated.getDescriptor();


    @Test
    public void shouldConvertToTimestampNtz() {
        Timestamp timestamp = Timestamp.newBuilder()
                .setSeconds(2500)
                .setNanos(100)
                .build();
        TestMaxComputeTypeInfo.TestRoot message = TestMaxComputeTypeInfo.TestRoot.newBuilder()
                .setTimestampField(timestamp)
                .build();
        java.sql.Timestamp expectedTimestamp = new java.sql.Timestamp(timestamp.getSeconds() * 1000);
        expectedTimestamp.setNanos(timestamp.getNanos());

        Object result = timestampPayloadConverter.convertSingular(descriptor.getFields().get(3), message.getField(descriptor.getFields().get(3)));

        Assertions.assertThat(result)
                .isEqualTo(expectedTimestamp);
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
        java.sql.Timestamp expectedTimestamp1 = new java.sql.Timestamp(timestamp1.getSeconds() * 1000);
        expectedTimestamp1.setNanos(timestamp1.getNanos());
        java.sql.Timestamp expectedTimestamp2 = new java.sql.Timestamp(timestamp2.getSeconds() * 1000);
        expectedTimestamp2.setNanos(timestamp2.getNanos());

        Object result = timestampPayloadConverter.convert(repeatedDescriptor.getFields().get(3), message.getField(repeatedDescriptor.getFields().get(3)));

        Assertions.assertThat(result)
                .isInstanceOf(List.class);
        Assertions.assertThat(((List<?>) result).stream().map(java.sql.Timestamp.class::cast))
                .hasSize(2)
                .containsExactly(expectedTimestamp1, expectedTimestamp2);
    }

}
