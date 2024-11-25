package com.gotocompany.depot.maxcompute.schema.partition;

import com.aliyun.odps.Column;
import com.aliyun.odps.PartitionSpec;
import com.gotocompany.depot.maxcompute.schema.MaxComputeSchemaCache;

public interface PartitioningStrategy {
    String getOriginalPartitionColumnName();
    boolean shouldReplaceOriginalColumn();
    Column getPartitionColumn();
    PartitionSpec getPartitionSpec(Object object);
}
