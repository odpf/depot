package com.gotocompany.depot.maxcompute.schema.partition;

import com.aliyun.odps.Column;
import com.aliyun.odps.type.TypeInfoFactory;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

public class DefaultPartitioningStrategyTest {
    @Test
    public void shouldReturnOriginalPartitionColumnName() {
        DefaultPartitioningStrategy defaultPartitioningStrategy = new DefaultPartitioningStrategy(TypeInfoFactory.STRING,
                getMaxComputeSinkConfig());

        assertEquals("tablePartitionKey", defaultPartitioningStrategy.getOriginalPartitionColumnName());
    }

    @Test
    public void shouldReturnPartitionColumn() {
        MaxComputeSinkConfig maxComputeSinkConfig = getMaxComputeSinkConfig();
        DefaultPartitioningStrategy defaultPartitioningStrategy = new DefaultPartitioningStrategy(TypeInfoFactory.STRING,
                maxComputeSinkConfig);
        Column expectedColumn = Column.newBuilder(maxComputeSinkConfig.getTablePartitionColumnName(), TypeInfoFactory.STRING)
                .build();

        assertEquals(expectedColumn, defaultPartitioningStrategy.getPartitionColumn());
    }

    @Test
    public void shouldReturnTrueForReplacingOriginalColumn() {
        DefaultPartitioningStrategy defaultPartitioningStrategy = new DefaultPartitioningStrategy(TypeInfoFactory.STRING,
                getMaxComputeSinkConfig());

        assertTrue(defaultPartitioningStrategy.shouldReplaceOriginalColumn());
    }

    @Test
    public void shouldReturnValidPartitionSpec() {
        DefaultPartitioningStrategy defaultPartitioningStrategy = new DefaultPartitioningStrategy(TypeInfoFactory.STRING,
                getMaxComputeSinkConfig());
        String partitionKey = "object";
        String expectedPartitionSpecStringRepresentation = "tablePartitionColumnName='object'";

        assertEquals(expectedPartitionSpecStringRepresentation,
                defaultPartitioningStrategy.getPartitionSpec(partitionKey)
                .toString());
    }

    @Test
    public void shouldReturnDefaultPartitionSpec() {
        String expectedPartitionSpecStringRepresentation = "tablePartitionColumnName='__NULL__'";
        DefaultPartitioningStrategy defaultPartitioningStrategy = new DefaultPartitioningStrategy(TypeInfoFactory.STRING,
                getMaxComputeSinkConfig());

        assertEquals(expectedPartitionSpecStringRepresentation,
                defaultPartitioningStrategy.getPartitionSpec(null)
                .toString());
    }

    private MaxComputeSinkConfig getMaxComputeSinkConfig() {
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        when(maxComputeSinkConfig.getTablePartitionColumnName())
                .thenReturn("tablePartitionColumnName");
        when(maxComputeSinkConfig.getTablePartitionKey())
                .thenReturn("tablePartitionKey");
        return maxComputeSinkConfig;
    }
}
