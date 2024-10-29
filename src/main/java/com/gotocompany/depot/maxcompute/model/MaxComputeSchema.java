package com.gotocompany.depot.maxcompute.model;

import com.aliyun.odps.Column;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.type.TypeInfo;
import com.google.protobuf.Descriptors;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.RequiredArgsConstructor;

import java.util.Map;
import java.util.Objects;

@Data
@AllArgsConstructor
@RequiredArgsConstructor
@Builder
public class MaxComputeSchema {
    private final Descriptors.Descriptor descriptor;
    private final TableSchema tableSchema;
    private final Map<String, TypeInfo> dataColumns;
    private final Map<String, TypeInfo> metadataColumns;
    private final Map<String, TypeInfo> partitionColumns;
    private Column[] columns;

    public Column[] getColumns() {
        if (Objects.isNull(columns)) {
            columns = tableSchema.getColumns().toArray(new Column[]{});
        }
        return columns;
    }

}
