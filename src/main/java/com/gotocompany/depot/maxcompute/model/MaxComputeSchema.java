package com.gotocompany.depot.maxcompute.model;

import com.aliyun.odps.Column;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.type.TypeInfo;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

import java.util.Map;
import java.util.Objects;

@AllArgsConstructor
@Builder
@Getter
public class MaxComputeSchema {
    private final TableSchema tableSchema;
    private final Map<String, TypeInfo> dataColumns;
    private final Map<String, TypeInfo> metadataColumns;
    private Column[] columns;

    public Column[] getColumns() {
        if (Objects.isNull(columns)) {
            columns = tableSchema.getColumns().toArray(new Column[]{});
        }
        return columns;
    }

}
