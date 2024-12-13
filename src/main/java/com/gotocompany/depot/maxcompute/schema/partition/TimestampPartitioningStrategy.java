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

    /**
     * Get the original partition column name which is the key in the message payload.
     *
     * @return original partition column name
     */
    @Override
    public String getOriginalPartitionColumnName() {
        return partitionColumnKey;
    }

    /**
     * Timestamp partitioning strategy does not replace the original column.
     * Original timestamp field is retained.
     *
     * @return false
     */
    @Override
    public boolean shouldReplaceOriginalColumn() {
        return false;
    }

    /**
     * Get the partition column.
     *
     * @return partition column
     */
    @Override
    public Column getPartitionColumn() {
        Column column = Column.newBuilder(partitionColumnName, TypeInfoFactory.STRING)
                .build();
        column.setGenerateExpression(new TruncTime(partitionColumnKey, tablePartitionByTimestampTimeUnit));
        return column;
    }

    /**
     * To get the PartitionSpec that uses built in spec generator based on the payload.
     *
     * @param object the object for which the partition spec is to be generated
     * @return partition spec
     */
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
