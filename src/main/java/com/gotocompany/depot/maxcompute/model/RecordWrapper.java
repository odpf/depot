package com.gotocompany.depot.maxcompute.model;

import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.data.Record;
import com.gotocompany.depot.error.ErrorInfo;
import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class RecordWrapper {
    private Record record;
    private long index;
    private ErrorInfo errorInfo;
    private PartitionSpec partitionSpec;
}
