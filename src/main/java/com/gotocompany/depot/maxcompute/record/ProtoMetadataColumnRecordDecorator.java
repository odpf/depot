package com.gotocompany.depot.maxcompute.record;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.SimpleStruct;
import com.aliyun.odps.type.StructTypeInfo;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.utils.StringUtils;
import com.gotocompany.depot.common.TupleString;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.maxcompute.model.MaxComputeSchema;
import com.gotocompany.depot.maxcompute.model.RecordWrapper;
import com.gotocompany.depot.maxcompute.schema.MaxComputeSchemaCache;
import com.gotocompany.depot.maxcompute.util.MetadataUtil;
import com.gotocompany.depot.message.Message;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ProtoMetadataColumnRecordDecorator extends RecordDecorator {

    private final MaxComputeSinkConfig maxComputeSinkConfig;
    private final MaxComputeSchemaCache maxComputeSchemaCache;
    private final Map<String, String> metadataTypePairs;

    public ProtoMetadataColumnRecordDecorator(RecordDecorator recordDecorator,
                                              MaxComputeSinkConfig maxComputeSinkConfig,
                                              MaxComputeSchemaCache maxComputeSchemaCache) {
        super(recordDecorator);
        this.maxComputeSinkConfig = maxComputeSinkConfig;
        this.maxComputeSchemaCache = maxComputeSchemaCache;
        this.metadataTypePairs = maxComputeSinkConfig.getMetadataColumnsTypes()
                .stream()
                .collect(Collectors.toMap(TupleString::getFirst, TupleString::getSecond));
    }

    @Override
    public RecordWrapper process(RecordWrapper recordWrapper, Message message) throws IOException {
        if (StringUtils.isNotBlank(maxComputeSinkConfig.getMaxcomputeMetadataNamespace())) {
            appendNamespacedMetadata(recordWrapper.getRecord(), message);
        } else {
            appendMetadata(recordWrapper.getRecord(), message);
        }
        return new RecordWrapper(recordWrapper.getRecord(), recordWrapper.getIndex(), recordWrapper.getErrorInfo(), recordWrapper.getPartitionSpec());
    }

    private void appendNamespacedMetadata(Record record, Message message) {
        Map<String, Object> metadata = message.getMetadata(maxComputeSinkConfig.getMetadataColumnsTypes());
        MaxComputeSchema maxComputeSchema = maxComputeSchemaCache.getMaxComputeSchema();
        StructTypeInfo typeInfo = (StructTypeInfo) maxComputeSchema.getTableSchema()
                .getColumn(maxComputeSinkConfig.getMaxcomputeMetadataNamespace())
                .getTypeInfo();
        List<Object> values = IntStream.range(0, typeInfo.getFieldCount())
                .mapToObj(index -> {
                    Object metadataValue = metadata.get(typeInfo.getFieldNames().get(index));
                    return MetadataUtil.getValidMetadataValue(metadataTypePairs.get(typeInfo.getFieldNames().get(index)), metadataValue, maxComputeSinkConfig);
                }).collect(Collectors.toList());
        record.set(maxComputeSinkConfig.getMaxcomputeMetadataNamespace(), new SimpleStruct(typeInfo, values));
    }

    private void appendMetadata(Record record, Message message) {
        Map<String, Object> metadata = message.getMetadata(maxComputeSinkConfig.getMetadataColumnsTypes());
        for (Map.Entry<String, TypeInfo> entry : maxComputeSchemaCache.getMaxComputeSchema()
                .getMetadataColumns()
                .entrySet()) {
            Object value = metadata.get(entry.getKey());
            record.set(entry.getKey(), MetadataUtil.getValidMetadataValue(metadataTypePairs.get(entry.getKey()), value, maxComputeSinkConfig));
        }
    }

}
