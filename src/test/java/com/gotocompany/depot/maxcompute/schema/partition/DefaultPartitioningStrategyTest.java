package com.gotocompany.depot.maxcompute.schema.partition;

import com.aliyun.odps.Column;
import com.aliyun.odps.type.TypeInfoFactory;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.mockito.Mockito;

public class DefaultPartitioningStrategyTest {
    @Test
    public void shouldReturnOriginalPartitionColumnName() {
        DefaultPartitioningStrategy defaultPartitioningStrategy = new DefaultPartitioningStrategy(TypeInfoFactory.STRING,
                getMaxComputeSinkConfig());

        Assertions.assertEquals("tablePartitionKey", defaultPartitioningStrategy.getOriginalPartitionColumnName());
    }

    @Test
    public void shouldReturnPartitionColumn() {
        MaxComputeSinkConfig maxComputeSinkConfig = getMaxComputeSinkConfig();
        DefaultPartitioningStrategy defaultPartitioningStrategy = new DefaultPartitioningStrategy(TypeInfoFactory.STRING,
                maxComputeSinkConfig);
        Column expectedColumn = Column.newBuilder(maxComputeSinkConfig.getTablePartitionColumnName(), TypeInfoFactory.STRING)
                .build();

        Assertions.assertEquals(expectedColumn, defaultPartitioningStrategy.getPartitionColumn());
    }

    @Test
    public void shouldReturnTrueForReplacingOriginalColumn() {
        DefaultPartitioningStrategy defaultPartitioningStrategy = new DefaultPartitioningStrategy(TypeInfoFactory.STRING,
                getMaxComputeSinkConfig());

        Assertions.assertTrue(defaultPartitioningStrategy.shouldReplaceOriginalColumn());
    }

    @Test
    public void shouldReturnValidPartitionSpec() {
        DefaultPartitioningStrategy defaultPartitioningStrategy = new DefaultPartitioningStrategy(TypeInfoFactory.STRING,
                getMaxComputeSinkConfig());
        String partitionKey = "object";
        String expectedPartitionSpecStringRepresentation = "tablePartitionColumnName='object'";

        Assertions.assertEquals(expectedPartitionSpecStringRepresentation,
                defaultPartitioningStrategy.getPartitionSpec(partitionKey)
                .toString());
    }

    @Test
    public void shouldReturnDefaultPartitionSpec() {
        String expectedPartitionSpecStringRepresentation = "tablePartitionColumnName='DEFAULT'";
        DefaultPartitioningStrategy defaultPartitioningStrategy = new DefaultPartitioningStrategy(TypeInfoFactory.STRING,
                getMaxComputeSinkConfig());

        Assertions.assertEquals(expectedPartitionSpecStringRepresentation,
                defaultPartitioningStrategy.getPartitionSpec(null)
                .toString());
    }

    private MaxComputeSinkConfig getMaxComputeSinkConfig() {
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        Mockito.when(maxComputeSinkConfig.getTablePartitionColumnName())
                .thenReturn("tablePartitionColumnName");
        Mockito.when(maxComputeSinkConfig.getTablePartitionKey())
                .thenReturn("tablePartitionKey");
        return maxComputeSinkConfig;
    }
}
