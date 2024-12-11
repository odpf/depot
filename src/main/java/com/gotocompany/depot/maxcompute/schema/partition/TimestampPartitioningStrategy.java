package com.gotocompany.depot.maxcompute.schema.partition;

import com.aliyun.odps.Column;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.data.GenerateExpression;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.expression.TruncTime;
import com.aliyun.odps.type.TypeInfoFactory;
import com.gotocompany.depot.config.MaxComputeSinkConfig;

public class TimestampPartitioningStrategy implements PartitioningStrategy {

    private final GenerateExpression generateExpression;
    private final String partitionColumnName;
    private final String partitionColumnKey;
    private final String tablePartitionByTimestampTimeUnit;

    public TimestampPartitioningStrategy(MaxComputeSinkConfig maxComputeSinkConfig) {
        this.partitionColumnName = maxComputeSinkConfig.getTablePartitionColumnName();
        this.partitionColumnKey = maxComputeSinkConfig.getTablePartitionKey();
        this.tablePartitionByTimestampTimeUnit = maxComputeSinkConfig.getTablePartitionByTimestampTimeUnit();
        this.generateExpression = initializeGenerateExpression();
    }

    @Override
    public String getOriginalPartitionColumnName() {
        return partitionColumnKey;
    }

    @Override
    public boolean shouldReplaceOriginalColumn() {
        return false;
    }

    @Override
    public Column getPartitionColumn() {
        Column column = Column.newBuilder(partitionColumnName, TypeInfoFactory.STRING)
                .build();
        column.setGenerateExpression(new TruncTime(partitionColumnKey, tablePartitionByTimestampTimeUnit));
        return column;
    }

    @Override
    public PartitionSpec getPartitionSpec(Object object) {
        PartitionSpec partitionSpec = new PartitionSpec();
        if (object instanceof Record) {
            Record record = (Record) object;
            partitionSpec.set(partitionColumnName, generateExpression.generate(record));
        }
        return partitionSpec;
    }

    private GenerateExpression initializeGenerateExpression() {
        return new TruncTime(partitionColumnKey, tablePartitionByTimestampTimeUnit);
    }

}
