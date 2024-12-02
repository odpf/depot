package com.gotocompany.depot.maxcompute.schema.partition;

import com.google.protobuf.Descriptors;
import com.gotocompany.depot.TestMaxComputePartition;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.maxcompute.converter.ProtobufConverterOrchestrator;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.time.ZoneId;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

public class PartitioningStrategyFactoryTest {

    private final Descriptors.Descriptor descriptor = TestMaxComputePartition.MaxComputePartition.getDescriptor();

    @Test
    public void shouldReturnDefaultPartitionStrategy() {
        String stringFieldName = "string_field";
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        when(maxComputeSinkConfig.isTablePartitioningEnabled()).thenReturn(true);
        when(maxComputeSinkConfig.getTablePartitionKey()).thenReturn(stringFieldName);
        when(maxComputeSinkConfig.getTablePartitionColumnName()).thenReturn(stringFieldName);
        when(maxComputeSinkConfig.getZoneId()).thenReturn(ZoneId.of("UTC"));

        PartitioningStrategy partitioningStrategy = PartitioningStrategyFactory.createPartitioningStrategy(
                new ProtobufConverterOrchestrator(maxComputeSinkConfig),
                maxComputeSinkConfig,
                descriptor
        );

        assertTrue(partitioningStrategy instanceof DefaultPartitioningStrategy);
    }

    @Test
    public void shouldReturnTimestampPartitionStrategy() {
        String timestampFieldName = "timestamp_field";
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        when(maxComputeSinkConfig.isTablePartitioningEnabled()).thenReturn(true);
        when(maxComputeSinkConfig.getTablePartitionKey()).thenReturn(timestampFieldName);
        when(maxComputeSinkConfig.getTablePartitionColumnName()).thenReturn(timestampFieldName);
        when(maxComputeSinkConfig.getTablePartitionByTimestampTimeUnit()).thenReturn("DAY");
        when(maxComputeSinkConfig.getZoneId()).thenReturn(ZoneId.of("UTC"));

        PartitioningStrategy partitioningStrategy = PartitioningStrategyFactory.createPartitioningStrategy(
                new ProtobufConverterOrchestrator(maxComputeSinkConfig),
                maxComputeSinkConfig,
                descriptor
        );

        assertTrue(partitioningStrategy instanceof TimestampPartitioningStrategy);
    }

    @Test
    public void shouldReturnNull() {
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        when(maxComputeSinkConfig.isTablePartitioningEnabled()).thenReturn(Boolean.FALSE);
        when(maxComputeSinkConfig.getZoneId()).thenReturn(ZoneId.of("UTC"));

        PartitioningStrategy partitioningStrategy = PartitioningStrategyFactory.createPartitioningStrategy(
                new ProtobufConverterOrchestrator(maxComputeSinkConfig),
                maxComputeSinkConfig,
                descriptor
        );

        Assert.assertNull(partitioningStrategy);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIllegalArgumentExceptionWhenTypeInfoIsNotSupported() {
        String unsupportedTypeFieldName = "float_field";
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        when(maxComputeSinkConfig.isTablePartitioningEnabled()).thenReturn(true);
        when(maxComputeSinkConfig.getTablePartitionKey()).thenReturn(unsupportedTypeFieldName);
        when(maxComputeSinkConfig.getTablePartitionColumnName()).thenReturn(unsupportedTypeFieldName);
        when(maxComputeSinkConfig.getZoneId()).thenReturn(ZoneId.of("UTC"));

        PartitioningStrategyFactory.createPartitioningStrategy(
                new ProtobufConverterOrchestrator(maxComputeSinkConfig),
                maxComputeSinkConfig,
                descriptor
        );
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIllegalArgumentExceptionWhenFieldIsNotFoundInDescriptor() {
        String fieldName = "non_existent_field";
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        when(maxComputeSinkConfig.isTablePartitioningEnabled()).thenReturn(true);
        when(maxComputeSinkConfig.getTablePartitionKey()).thenReturn(fieldName);
        when(maxComputeSinkConfig.getTablePartitionColumnName()).thenReturn(fieldName);
        when(maxComputeSinkConfig.getZoneId()).thenReturn(ZoneId.of("UTC"));

        PartitioningStrategyFactory.createPartitioningStrategy(
                new ProtobufConverterOrchestrator(maxComputeSinkConfig),
                maxComputeSinkConfig,
                descriptor
        );
    }

}
