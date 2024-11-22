package com.gotocompany.depot.maxcompute.schema.partition;

import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.protobuf.Descriptors;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.maxcompute.converter.ConverterOrchestrator;
import lombok.RequiredArgsConstructor;

import java.util.HashSet;
import java.util.Set;

@RequiredArgsConstructor
public class PartitioningStrategyFactory {

    private static final Set<TypeInfo> ALLOWED_PARTITION_KEY_TYPE_INFO;

    static {
        ALLOWED_PARTITION_KEY_TYPE_INFO = new HashSet<>();
        ALLOWED_PARTITION_KEY_TYPE_INFO.add(TypeInfoFactory.TIMESTAMP_NTZ);
        ALLOWED_PARTITION_KEY_TYPE_INFO.add(TypeInfoFactory.STRING);
        ALLOWED_PARTITION_KEY_TYPE_INFO.add(TypeInfoFactory.TINYINT);
        ALLOWED_PARTITION_KEY_TYPE_INFO.add(TypeInfoFactory.SMALLINT);
        ALLOWED_PARTITION_KEY_TYPE_INFO.add(TypeInfoFactory.INT);
        ALLOWED_PARTITION_KEY_TYPE_INFO.add(TypeInfoFactory.BIGINT);
    }

    public static PartitioningStrategy createPartitioningStrategy(
            ConverterOrchestrator converterOrchestrator,
            MaxComputeSinkConfig maxComputeSinkConfig,
            Descriptors.Descriptor descriptor) {
        if (!maxComputeSinkConfig.isTablePartitioningEnabled()) {
            return null;
        }
        String partitionKey = maxComputeSinkConfig.getTablePartitionKey();
        Descriptors.FieldDescriptor fieldDescriptor = descriptor
                .findFieldByName(partitionKey);
        if (fieldDescriptor == null) {
            throw new IllegalArgumentException("Partition key not found in the descriptor: " + partitionKey);
        }
        TypeInfo partitionKeyTypeInfo = converterOrchestrator.convert(fieldDescriptor);
        checkPartitionTypePrecondition(partitionKeyTypeInfo);
        if (TypeInfoFactory.TIMESTAMP_NTZ.equals(partitionKeyTypeInfo)) {
            return new TimestampPartitioningStrategy(maxComputeSinkConfig);
        } else {
            return new DefaultPartitioningStrategy(partitionKeyTypeInfo, maxComputeSinkConfig);
        }
    }

    private static void checkPartitionTypePrecondition(TypeInfo typeInfo) {
        if (!ALLOWED_PARTITION_KEY_TYPE_INFO.contains(typeInfo)) {
            throw new IllegalArgumentException("Partition key type not supported: " + typeInfo.getTypeName());
        }
    }

}
