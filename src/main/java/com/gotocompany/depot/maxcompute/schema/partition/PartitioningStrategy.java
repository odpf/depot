package com.gotocompany.depot.maxcompute.schema.partition;

import com.aliyun.odps.Column;
import com.aliyun.odps.PartitionSpec;

/**
 * Interface to define the partitioning strategy for a table.
 */
public interface PartitioningStrategy {

    /**
     * Get the original partition column name, typically the field name in the object.
     * @return the original partition column name.
     */
    String getOriginalPartitionColumnName();

    /**
     * Flag to indicates that the partitioned column should replace the original column in the schema.
     * Currently MaxCompute requires an additional column to be added to the schema for time-partitioning, hence this flag.
     *
     * @return true if the partitioned column should replace the original column in the schema
     *        false if the partitioned column should be added to the schema
     */
    boolean shouldReplaceOriginalColumn();

    /**
     * Get the partition column.
     * @return the partition column
     */
    Column getPartitionColumn();

    /**
     * Get the partition spec for the object.
     * Method will generate the partition spec based on the partitioning strategy.
     *
     * @param object the object for which the partition spec is to be generated
     * @return the partition spec
     */
    PartitionSpec getPartitionSpec(Object object);
}
