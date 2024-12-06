package com.gotocompany.depot.maxcompute.model;

import com.aliyun.odps.Column;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.type.TypeInfo;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.Map;

@RequiredArgsConstructor
@Getter
public class MaxComputeSchema {

    private final TableSchema tableSchema;
    private final Map<String, TypeInfo> metadataColumns;

    public Column[] getColumns() {
        return tableSchema.getColumns().toArray(new Column[]{});
    }

}
