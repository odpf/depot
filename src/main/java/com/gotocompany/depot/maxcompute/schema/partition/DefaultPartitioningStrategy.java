package com.gotocompany.depot.maxcompute.schema.partition;

import com.aliyun.odps.Column;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.type.TypeInfo;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class DefaultPartitioningStrategy implements PartitioningStrategy {

    private static final String PARTITION_SPEC_FORMAT = "%s=%s";
    private static final String DEFAULT_PARTITION = "DEFAULT";

    private final TypeInfo typeInfo;
    private final MaxComputeSinkConfig maxComputeSinkConfig;

    @Override
    public String getOriginalPartitionColumnName() {
        return maxComputeSinkConfig.getTablePartitionKey();
    }

    @Override
    public boolean shouldReplaceOriginalColumn() {
        return true;
    }

    @Override
    public Column getPartitionColumn() {
        return Column.newBuilder(maxComputeSinkConfig.getTablePartitionColumnName(), typeInfo)
                .build();
    }

    @Override
    public PartitionSpec getPartitionSpec(Object object) {
        if (object == null) {
            return new PartitionSpec(String.format(PARTITION_SPEC_FORMAT, maxComputeSinkConfig.getTablePartitionColumnName(), DEFAULT_PARTITION));
        }
        return new PartitionSpec(String.format(PARTITION_SPEC_FORMAT, maxComputeSinkConfig.getTablePartitionColumnName(), object));
    }

}
