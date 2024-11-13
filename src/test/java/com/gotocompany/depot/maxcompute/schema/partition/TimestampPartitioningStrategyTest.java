package com.gotocompany.depot.maxcompute.schema.partition;

import com.aliyun.odps.Column;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.protobuf.Timestamp;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.mockito.Mockito;

public class TimestampPartitioningStrategyTest {

    @Test
    public void shouldReturnOriginalPartitionColumnName() {
        TimestampPartitioningStrategy defaultPartitioningStrategy =
                new TimestampPartitioningStrategy(getMaxComputeSinkConfig());

        Assertions.assertEquals("tablePartitionKey",
                defaultPartitioningStrategy.getOriginalPartitionColumnName());
    }

    @Test
    public void shouldReturnFalseForReplacingOriginalColumn() {
        TimestampPartitioningStrategy defaultPartitioningStrategy =
                new TimestampPartitioningStrategy(getMaxComputeSinkConfig());

        Assertions.assertFalse(defaultPartitioningStrategy.shouldReplaceOriginalColumn());
    }

    @Test
    public void shouldReturnValidColumn() {
        MaxComputeSinkConfig maxComputeSinkConfig = getMaxComputeSinkConfig();
        TimestampPartitioningStrategy timestampPartitioningStrategy =
                new TimestampPartitioningStrategy(maxComputeSinkConfig);

        Column column = Column.newBuilder(maxComputeSinkConfig.getTablePartitionColumnName(), TypeInfoFactory.STRING)
                .build();

        Assertions.assertEquals(column, timestampPartitioningStrategy.getPartitionColumn());
    }

    @Test
    public void shouldReturnValidPartitionSpec() {
        //October 29, 2024 12:00:10 AM GMT+07:00
        long epoch = 1730134810;
        Timestamp timestamp = Timestamp.newBuilder()
                .setSeconds(epoch)
                .setNanos(0)
                .build();
        MaxComputeSinkConfig maxComputeSinkConfig = getMaxComputeSinkConfig();
        TimestampPartitioningStrategy timestampPartitioningStrategy =
                new TimestampPartitioningStrategy(maxComputeSinkConfig);
        String expectedStartOfDayEpoch = "2024-10-28T00:00";

        Assertions.assertEquals(String.format("tablePartitionColumnName='%s'", expectedStartOfDayEpoch),
                timestampPartitioningStrategy.getPartitionSpec(timestamp).toString());
    }

    private MaxComputeSinkConfig getMaxComputeSinkConfig() {
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        Mockito.when(maxComputeSinkConfig.isTablePartitioningEnabled())
                .thenReturn(Boolean.TRUE);
        Mockito.when(maxComputeSinkConfig.getTablePartitionColumnName())
                .thenReturn("tablePartitionColumnName");
        Mockito.when(maxComputeSinkConfig.getTablePartitionKey())
                .thenReturn("tablePartitionKey");
        return maxComputeSinkConfig;
    }
}
