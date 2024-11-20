package com.gotocompany.depot.maxcompute.schema.partition;

import com.aliyun.odps.Column;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.expression.TruncTime;
import com.aliyun.odps.type.TypeInfoFactory;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.maxcompute.schema.MaxComputeSchemaCache;

public class TimestampPartitioningStrategy implements PartitioningStrategy {

    private static final String DAY = "DAY";

    private final MaxComputeSinkConfig maxComputeSinkConfig;
    private MaxComputeSchemaCache maxComputeSchemaCache;

    public TimestampPartitioningStrategy(MaxComputeSinkConfig maxComputeSinkConfig) {
        this.maxComputeSinkConfig = maxComputeSinkConfig;
    }

    @Override
    public String getOriginalPartitionColumnName() {
        return maxComputeSinkConfig.getTablePartitionKey();
    }

    @Override
    public boolean shouldReplaceOriginalColumn() {
        return false;
    }

    @Override
    public Column getPartitionColumn() {
        Column column = Column.newBuilder(maxComputeSinkConfig.getTablePartitionColumnName(), TypeInfoFactory.STRING)
                .build();
        column.setGenerateExpression(new TruncTime(maxComputeSinkConfig.getTablePartitionKey(), DAY));
        return column;
    }

    @Override
    public PartitionSpec getPartitionSpec(Object object) {
        PartitionSpec partitionSpec = new PartitionSpec();
        maxComputeSchemaCache.getMaxComputeSchema()
                .getTableSchema()
                .getPartitionColumns()
                .forEach(partitionColumn -> partitionSpec.set(partitionColumn.getName(), partitionColumn.getGenerateExpression()
                                .generate((Record) object)));
        return partitionSpec;
    }

    @Override
    public void setMaxComputeSchemaCache(MaxComputeSchemaCache maxComputeSchemaCache) {
        this.maxComputeSchemaCache = maxComputeSchemaCache;
    }

}
