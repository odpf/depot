package com.gotocompany.depot.maxcompute.schema.partition;

import com.google.protobuf.Descriptors;
import com.gotocompany.depot.TestMaxComputePartition;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.maxcompute.converter.ProtobufConverterOrchestrator;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.time.ZoneId;

public class PartitioningStrategyFactoryTest {

    private final Descriptors.Descriptor descriptor = TestMaxComputePartition.MaxComputePartition.getDescriptor();

    @Test
    public void shouldReturnDefaultPartitionStrategy() {
        String stringFieldName = "string_field";
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        Mockito.when(maxComputeSinkConfig.isTablePartitioningEnabled()).thenReturn(true);
        Mockito.when(maxComputeSinkConfig.getTablePartitionKey()).thenReturn(stringFieldName);
        Mockito.when(maxComputeSinkConfig.getTablePartitionColumnName()).thenReturn(stringFieldName);
        Mockito.when(maxComputeSinkConfig.getZoneId()).thenReturn(ZoneId.of("UTC"));

        PartitioningStrategy partitioningStrategy = PartitioningStrategyFactory.createPartitioningStrategy(
                new ProtobufConverterOrchestrator(maxComputeSinkConfig),
                maxComputeSinkConfig,
                descriptor
        );

        Assert.assertTrue(partitioningStrategy instanceof DefaultPartitioningStrategy);
    }

    @Test
    public void shouldReturnTimestampPartitionStrategy() {
        String timestampFieldName = "timestamp_field";
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        Mockito.when(maxComputeSinkConfig.isTablePartitioningEnabled()).thenReturn(true);
        Mockito.when(maxComputeSinkConfig.getTablePartitionKey()).thenReturn(timestampFieldName);
        Mockito.when(maxComputeSinkConfig.getTablePartitionColumnName()).thenReturn(timestampFieldName);
        Mockito.when(maxComputeSinkConfig.getTablePartitionByTimestampTimeUnit()).thenReturn("DAY");
        Mockito.when(maxComputeSinkConfig.getZoneId()).thenReturn(ZoneId.of("UTC"));

        PartitioningStrategy partitioningStrategy = PartitioningStrategyFactory.createPartitioningStrategy(
                new ProtobufConverterOrchestrator(maxComputeSinkConfig),
                maxComputeSinkConfig,
                descriptor
        );

        Assert.assertTrue(partitioningStrategy instanceof TimestampPartitioningStrategy);
    }

    @Test
    public void shouldReturnNull() {
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        Mockito.when(maxComputeSinkConfig.isTablePartitioningEnabled()).thenReturn(Boolean.FALSE);
        Mockito.when(maxComputeSinkConfig.getZoneId()).thenReturn(ZoneId.of("UTC"));

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
        Mockito.when(maxComputeSinkConfig.isTablePartitioningEnabled()).thenReturn(true);
        Mockito.when(maxComputeSinkConfig.getTablePartitionKey()).thenReturn(unsupportedTypeFieldName);
        Mockito.when(maxComputeSinkConfig.getTablePartitionColumnName()).thenReturn(unsupportedTypeFieldName);
        Mockito.when(maxComputeSinkConfig.getZoneId()).thenReturn(ZoneId.of("UTC"));

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
        Mockito.when(maxComputeSinkConfig.isTablePartitioningEnabled()).thenReturn(true);
        Mockito.when(maxComputeSinkConfig.getTablePartitionKey()).thenReturn(fieldName);
        Mockito.when(maxComputeSinkConfig.getTablePartitionColumnName()).thenReturn(fieldName);
        Mockito.when(maxComputeSinkConfig.getZoneId()).thenReturn(ZoneId.of("UTC"));

        PartitioningStrategyFactory.createPartitioningStrategy(
                new ProtobufConverterOrchestrator(maxComputeSinkConfig),
                maxComputeSinkConfig,
                descriptor
        );
    }

}
