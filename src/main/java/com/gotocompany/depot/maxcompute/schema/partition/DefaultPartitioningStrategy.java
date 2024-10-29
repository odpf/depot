package com.gotocompany.depot.maxcompute.schema.partition;

import com.aliyun.odps.Column;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.type.TypeInfo;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class DefaultPartitioningStrategy implements PartitioningStrategy {

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
        return new PartitionSpec(String.format("%s=%s", maxComputeSinkConfig.getTablePartitionColumnName(), object.toString()));
    }

}
