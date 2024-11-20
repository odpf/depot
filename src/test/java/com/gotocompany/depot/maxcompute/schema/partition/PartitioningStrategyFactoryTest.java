package com.gotocompany.depot.maxcompute.schema.partition;

import com.google.protobuf.Descriptors;
import com.gotocompany.depot.TestMaxComputePartition;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.maxcompute.converter.ConverterOrchestrator;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class PartitioningStrategyFactoryTest {

    private final Descriptors.Descriptor descriptor = TestMaxComputePartition.MaxComputePartition.getDescriptor();

    @Test
    public void shouldReturnDefaultPartitionStrategy() {
        String stringFieldName = "string_field";
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        Mockito.when(maxComputeSinkConfig.isTablePartitioningEnabled()).thenReturn(true);
        Mockito.when(maxComputeSinkConfig.getTablePartitionKey()).thenReturn(stringFieldName);
        Mockito.when(maxComputeSinkConfig.getTablePartitionColumnName()).thenReturn(stringFieldName);
        Mockito.when(maxComputeSinkConfig.getZoneOffset()).thenReturn("+00:00");
        PartitioningStrategyFactory partitioningStrategyFactory = new PartitioningStrategyFactory(new ConverterOrchestrator(maxComputeSinkConfig), maxComputeSinkConfig);

        PartitioningStrategy partitioningStrategy = partitioningStrategyFactory.createPartitioningStrategy(descriptor);

        Assert.assertTrue(partitioningStrategy instanceof DefaultPartitioningStrategy);
    }

    @Test
    public void shouldReturnTimestampPartitionStrategy() {
        String timestampFieldName = "timestamp_field";
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        Mockito.when(maxComputeSinkConfig.isTablePartitioningEnabled()).thenReturn(true);
        Mockito.when(maxComputeSinkConfig.getTablePartitionKey()).thenReturn(timestampFieldName);
        Mockito.when(maxComputeSinkConfig.getTablePartitionColumnName()).thenReturn(timestampFieldName);
        Mockito.when(maxComputeSinkConfig.getTablePartitionByTimestampKeyFormat()).thenReturn("YYYY-MM-dd'T'HH:mm");
        Mockito.when(maxComputeSinkConfig.getZoneOffset()).thenReturn("+00:00");
        PartitioningStrategyFactory partitioningStrategyFactory = new PartitioningStrategyFactory(new ConverterOrchestrator(maxComputeSinkConfig), maxComputeSinkConfig);

        PartitioningStrategy partitioningStrategy = partitioningStrategyFactory.createPartitioningStrategy(descriptor);

        Assert.assertTrue(partitioningStrategy instanceof TimestampPartitioningStrategy);
    }

    @Test
    public void shouldReturnNull() {
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        Mockito.when(maxComputeSinkConfig.isTablePartitioningEnabled()).thenReturn(Boolean.FALSE);
        Mockito.when(maxComputeSinkConfig.getZoneOffset()).thenReturn("+00:00");
        PartitioningStrategyFactory partitioningStrategyFactory = new PartitioningStrategyFactory(new ConverterOrchestrator(maxComputeSinkConfig), maxComputeSinkConfig);

        PartitioningStrategy partitioningStrategy = partitioningStrategyFactory.createPartitioningStrategy(descriptor);

        Assert.assertNull(partitioningStrategy);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIllegalArgumentExceptionWhenTypeInfoIsNotSupported() {
        String unsupportedTypeFieldName = "float_field";
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        Mockito.when(maxComputeSinkConfig.isTablePartitioningEnabled()).thenReturn(true);
        Mockito.when(maxComputeSinkConfig.getTablePartitionKey()).thenReturn(unsupportedTypeFieldName);
        Mockito.when(maxComputeSinkConfig.getTablePartitionColumnName()).thenReturn(unsupportedTypeFieldName);
        Mockito.when(maxComputeSinkConfig.getZoneOffset()).thenReturn("+00:00");
        PartitioningStrategyFactory partitioningStrategyFactory = new PartitioningStrategyFactory(new ConverterOrchestrator(maxComputeSinkConfig), maxComputeSinkConfig);

        partitioningStrategyFactory.createPartitioningStrategy(descriptor);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIllegalArgumentExceptionWhenFieldIsNotFoundInDescriptor() {
        String fieldName = "non_existent_field";
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        Mockito.when(maxComputeSinkConfig.isTablePartitioningEnabled()).thenReturn(true);
        Mockito.when(maxComputeSinkConfig.getTablePartitionKey()).thenReturn(fieldName);
        Mockito.when(maxComputeSinkConfig.getTablePartitionColumnName()).thenReturn(fieldName);
        Mockito.when(maxComputeSinkConfig.getZoneOffset()).thenReturn("+00:00");
        PartitioningStrategyFactory partitioningStrategyFactory = new PartitioningStrategyFactory(new ConverterOrchestrator(maxComputeSinkConfig), maxComputeSinkConfig);

        partitioningStrategyFactory.createPartitioningStrategy(descriptor);
    }

}
