package com.gotocompany.depot.maxcompute.helper;

import com.aliyun.odps.Column;
import com.aliyun.odps.TableSchema;
import com.google.protobuf.Descriptors;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.maxcompute.converter.ConverterOrchestrator;
import com.gotocompany.depot.maxcompute.model.MaxComputeSchema;
import com.gotocompany.depot.maxcompute.schema.partition.PartitioningStrategy;
import com.gotocompany.depot.maxcompute.util.MetadataUtil;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@RequiredArgsConstructor
public class MaxComputeSchemaHelper {

    private final ConverterOrchestrator converterOrchestrator;
    private final MaxComputeSinkConfig maxComputeSinkConfig;
    private final PartitioningStrategy partitioningStrategy;

    public MaxComputeSchema buildMaxComputeSchema(Descriptors.Descriptor descriptor) {

        List<Column> metadataColumns = buildMetadataColumns();
        List<Column> dataColumn = buildDataColumns(descriptor);
        Column partitionColumn = maxComputeSinkConfig.isTablePartitioningEnabled() ? buildPartitionColumn() : null;
        TableSchema.Builder tableSchemaBuilder = com.aliyun.odps.TableSchema.builder();
        tableSchemaBuilder.withColumns(dataColumn);
        tableSchemaBuilder.withColumns(metadataColumns);
        if (Objects.nonNull(partitionColumn)) {
            tableSchemaBuilder.withPartitionColumn(partitionColumn);
        }
        return MaxComputeSchema.builder()
                .tableSchema(tableSchemaBuilder.build())
                .dataColumns(dataColumn.stream().collect(Collectors.toMap(Column::getName, Column::getTypeInfo)))
                .metadataColumns(metadataColumns.stream().collect(Collectors.toMap(Column::getName, Column::getTypeInfo)))
                .build();

    }

    private List<Column> buildDataColumns(Descriptors.Descriptor descriptor) {
        return descriptor.getFields()
                .stream()
                .filter(fieldDescriptor -> {
                    if (!maxComputeSinkConfig.isTablePartitioningEnabled() || !fieldDescriptor.getName().equals(maxComputeSinkConfig.getTablePartitionKey())) {
                        return true;
                    }
                    return !partitioningStrategy.shouldReplaceOriginalColumn();
                })
                .map(fieldDescriptor -> Column.newBuilder(fieldDescriptor.getName(),
                        converterOrchestrator.convert(fieldDescriptor)).build())
                .collect(Collectors.toList());
    }

    private Column buildPartitionColumn() {
        return partitioningStrategy.getPartitionColumn();
    }

    private List<Column> buildMetadataColumns() {
        if (!maxComputeSinkConfig.shouldAddMetadata()) {
            return new ArrayList<>();
        }
        if (StringUtils.isBlank(maxComputeSinkConfig.getMaxcomputeMetadataNamespace())) {
            return maxComputeSinkConfig.getMetadataColumnsTypes()
                    .stream()
                    .map(tuple -> Column.newBuilder(tuple.getFirst(), MetadataUtil.getMetadataTypeInfo(tuple.getSecond())).build())
                    .collect(Collectors.toList());
        }
        return Collections.singletonList(Column.newBuilder(maxComputeSinkConfig.getMaxcomputeMetadataNamespace(),
                MetadataUtil.getMetadataTypeInfo(maxComputeSinkConfig.getMetadataColumnsTypes())).build());
    }

}
