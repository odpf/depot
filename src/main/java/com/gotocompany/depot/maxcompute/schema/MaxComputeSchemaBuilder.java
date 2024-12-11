package com.gotocompany.depot.maxcompute.schema;

import com.aliyun.odps.Column;
import com.aliyun.odps.TableSchema;
import com.google.protobuf.Descriptors;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.maxcompute.converter.ProtobufConverterOrchestrator;
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
public class MaxComputeSchemaBuilder {

    private final ProtobufConverterOrchestrator protobufConverterOrchestrator;
    private final MaxComputeSinkConfig maxComputeSinkConfig;
    private final PartitioningStrategy partitioningStrategy;

    public MaxComputeSchema build(Descriptors.Descriptor descriptor) {
        List<Column> metadataColumns = buildMetadataColumns();
        TableSchema.Builder tableSchemaBuilder = com.aliyun.odps.TableSchema.builder()
                .withColumns(metadataColumns)
                .withColumns(buildDataColumns(descriptor));
        Column partitionColumn = maxComputeSinkConfig.isTablePartitioningEnabled() ? buildPartitionColumn() : null;
        if (Objects.nonNull(partitionColumn)) {
            tableSchemaBuilder.withPartitionColumn(partitionColumn);
        }
        return new MaxComputeSchema(
                tableSchemaBuilder.build(),
                metadataColumns.stream().collect(Collectors.toMap(Column::getName, Column::getTypeInfo))
        );

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
                .map(fieldDescriptor -> Column.newBuilder(fieldDescriptor.getName(), protobufConverterOrchestrator.toMaxComputeTypeInfo(fieldDescriptor)).build())
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
