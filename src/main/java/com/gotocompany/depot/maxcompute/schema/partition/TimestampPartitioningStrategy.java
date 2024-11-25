package com.gotocompany.depot.maxcompute.schema.partition;

import com.aliyun.odps.Column;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.data.GenerateExpression;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.expression.TruncTime;
import com.aliyun.odps.type.TypeInfoFactory;
import com.gotocompany.depot.config.MaxComputeSinkConfig;

public class TimestampPartitioningStrategy implements PartitioningStrategy {

    private final MaxComputeSinkConfig maxComputeSinkConfig;
    private final GenerateExpression generateExpression;

    public TimestampPartitioningStrategy(MaxComputeSinkConfig maxComputeSinkConfig) {
        this.maxComputeSinkConfig = maxComputeSinkConfig;
        this.generateExpression = initializeGenerateExpression();
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
        column.setGenerateExpression(new TruncTime(maxComputeSinkConfig.getTablePartitionKey(),
                maxComputeSinkConfig.getTablePartitionByTimestampTimeUnit()));
        return column;
    }

    @Override
    public PartitionSpec getPartitionSpec(Object object) {
        PartitionSpec partitionSpec = new PartitionSpec();
        if (object instanceof Record) {
            Record record = (Record) object;
            partitionSpec.set(maxComputeSinkConfig.getTablePartitionColumnName(), generateExpression.generate(record));
        }
        return partitionSpec;
    }

    private GenerateExpression initializeGenerateExpression() {
        return new TruncTime(maxComputeSinkConfig.getTablePartitionKey(),
                maxComputeSinkConfig.getTablePartitionByTimestampTimeUnit());
    }

}
