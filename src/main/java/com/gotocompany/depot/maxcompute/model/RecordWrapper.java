package com.gotocompany.depot.maxcompute.model;

import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.data.Record;
import com.gotocompany.depot.error.ErrorInfo;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@Getter
public class RecordWrapper {
    private final Record record;
    private final long index;
    private final ErrorInfo errorInfo;
    private final PartitionSpec partitionSpec;
}
