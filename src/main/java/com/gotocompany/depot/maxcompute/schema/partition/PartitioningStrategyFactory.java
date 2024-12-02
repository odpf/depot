package com.gotocompany.depot.maxcompute.schema.partition;

import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.common.collect.Sets;
import com.google.protobuf.Descriptors;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.maxcompute.converter.ProtobufConverterOrchestrator;
import lombok.RequiredArgsConstructor;

import java.util.Set;

@RequiredArgsConstructor
public class PartitioningStrategyFactory {

    private static final Set<TypeInfo> ALLOWED_PARTITION_KEY_TYPE_INFO = Sets.newHashSet(
            TypeInfoFactory.TIMESTAMP_NTZ,
            TypeInfoFactory.STRING,
            TypeInfoFactory.TINYINT,
            TypeInfoFactory.SMALLINT,
            TypeInfoFactory.INT,
            TypeInfoFactory.BIGINT
    );

    public static PartitioningStrategy createPartitioningStrategy(
            ProtobufConverterOrchestrator protobufConverterOrchestrator,
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
        TypeInfo partitionKeyTypeInfo = protobufConverterOrchestrator.convert(fieldDescriptor);
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
