package com.gotocompany.depot.maxcompute.schema.partition;

import com.aliyun.odps.Column;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.expression.TruncTime;
import com.aliyun.odps.type.TypeInfoFactory;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.maxcompute.model.MaxComputeSchema;
import com.gotocompany.depot.maxcompute.schema.MaxComputeSchemaCache;
import org.junit.Test;
import org.mockito.Mockito;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.when;

public class TimestampPartitioningStrategyTest {

    @Test
    public void shouldReturnOriginalPartitionColumnName() {
        TimestampPartitioningStrategy defaultPartitioningStrategy =
                new TimestampPartitioningStrategy(getMaxComputeSinkConfig());

        assertEquals("event_timestamp",
                defaultPartitioningStrategy.getOriginalPartitionColumnName());
    }

    @Test
    public void shouldReturnFalseForReplacingOriginalColumn() {
        TimestampPartitioningStrategy defaultPartitioningStrategy =
                new TimestampPartitioningStrategy(getMaxComputeSinkConfig());

        assertFalse(defaultPartitioningStrategy.shouldReplaceOriginalColumn());
    }

    @Test
    public void shouldReturnValidColumn() {
        MaxComputeSinkConfig maxComputeSinkConfig = getMaxComputeSinkConfig();
        TimestampPartitioningStrategy timestampPartitioningStrategy =
                new TimestampPartitioningStrategy(maxComputeSinkConfig);

        Column column = Column.newBuilder(maxComputeSinkConfig.getTablePartitionColumnName(), TypeInfoFactory.STRING)
                .build();

        assertEquals(column, timestampPartitioningStrategy.getPartitionColumn());
    }

    @Test
    public void shouldReturnValidPartitionSpec() {
        //October 29, 2024 12:00:10 AM GMT+07:00
        long epoch = 1730134810;
        MaxComputeSinkConfig maxComputeSinkConfig = getMaxComputeSinkConfig();
        TimestampPartitioningStrategy timestampPartitioningStrategy =
                new TimestampPartitioningStrategy(maxComputeSinkConfig);
        Column partitionColumn = Column.newBuilder("tablePartitionColumnName", TypeInfoFactory.STRING)
                .build();
        partitionColumn.setGenerateExpression(new TruncTime("event_timestamp", "DAY"));
        TableSchema tableSchema = TableSchema.builder()
                .withStringColumn("str")
                .withColumn(Column.newBuilder("event_timestamp", TypeInfoFactory.TIMESTAMP_NTZ)
                        .build())
                .withPartitionColumn(partitionColumn)
                .build();
        MaxComputeSchemaCache maxComputeSchemaCache = Mockito.mock(MaxComputeSchemaCache.class);
        MaxComputeSchema maxComputeSchema = Mockito.mock(MaxComputeSchema.class);
        when(maxComputeSchema.getTableSchema()).thenReturn(tableSchema);
        when(maxComputeSchemaCache.getMaxComputeSchema())
                .thenReturn(maxComputeSchema);
        String expectedStartOfDayEpoch = "2024-10-28";
        Record record = new ArrayRecord(tableSchema);
        record.set("str", "strVal");
        record.set("event_timestamp", LocalDateTime.ofEpochSecond(epoch, 0, ZoneOffset.UTC));

        assertEquals(String.format("tablePartitionColumnName='%s'", expectedStartOfDayEpoch),
                timestampPartitioningStrategy.getPartitionSpec(record).toString());
    }

    @Test
    public void shouldEmptyPartitionSpecIfObjectIsNotRecord() {
        MaxComputeSinkConfig maxComputeSinkConfig = getMaxComputeSinkConfig();
        TimestampPartitioningStrategy timestampPartitioningStrategy =
                new TimestampPartitioningStrategy(maxComputeSinkConfig);

        assertEquals("",
                timestampPartitioningStrategy.getPartitionSpec("").toString());
    }

    @Test
    public void shouldReturnDefaultPartitionSpec() {
        String expectedPartitionSpecStringRepresentation = "tablePartitionColumnName='__NULL__'";
        TimestampPartitioningStrategy timestampPartitioningStrategy = new TimestampPartitioningStrategy(getMaxComputeSinkConfig());
        MaxComputeSchemaCache maxComputeSchemaCache = Mockito.mock(MaxComputeSchemaCache.class);
        MaxComputeSchema maxComputeSchema = Mockito.mock(MaxComputeSchema.class);
        Column partitionColumn = Column.newBuilder("tablePartitionColumnName", TypeInfoFactory.STRING)
                .build();
        partitionColumn.setGenerateExpression(new TruncTime("event_timestamp", "DAY"));
        TableSchema tableSchema = TableSchema.builder()
                .withStringColumn("str")
                .withDatetimeColumn("event_timestamp")
                .withPartitionColumn(partitionColumn)
                .build();
        when(maxComputeSchema.getTableSchema()).thenReturn(tableSchema);
        when(maxComputeSchemaCache.getMaxComputeSchema())
                .thenReturn(maxComputeSchema);
        Record record = new ArrayRecord(tableSchema);
        record.set("str", "strVal");
        record.set("event_timestamp", null);

        assertEquals(expectedPartitionSpecStringRepresentation,
                timestampPartitioningStrategy.getPartitionSpec(record)
                        .toString());
    }

    private MaxComputeSinkConfig getMaxComputeSinkConfig() {
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        when(maxComputeSinkConfig.isTablePartitioningEnabled())
                .thenReturn(Boolean.TRUE);
        when(maxComputeSinkConfig.getTablePartitionColumnName())
                .thenReturn("tablePartitionColumnName");
        when(maxComputeSinkConfig.getTablePartitionKey())
                .thenReturn("event_timestamp");
        when(maxComputeSinkConfig.getTablePartitionByTimestampTimeUnit())
                .thenReturn("DAY");
        return maxComputeSinkConfig;
    }

}
