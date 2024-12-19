package com.gotocompany.depot.maxcompute.converter;

import com.aliyun.odps.type.TypeInfo;
import com.google.protobuf.Descriptors;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.maxcompute.model.ProtoPayload;

/**
 * Orchestrates the conversion of Protobuf fields to MaxCompute record fields.
 * It uses a cache to store the converters for each field descriptor.
 */
public class ProtobufConverterOrchestrator {

    private final MaxComputeProtobufConverterCache maxComputeProtobufConverterCache;

    public ProtobufConverterOrchestrator(MaxComputeSinkConfig maxComputeSinkConfig) {
        maxComputeProtobufConverterCache = new MaxComputeProtobufConverterCache(maxComputeSinkConfig);
    }

    /**
     * Converts a Protobuf field to a MaxCompute TypeInfo.
     *
     * @param fieldDescriptor the Protobuf field descriptor
     * @return the MaxCompute TypeInfo
     */
    public TypeInfo toMaxComputeTypeInfo(Descriptors.FieldDescriptor fieldDescriptor) {
        return maxComputeProtobufConverterCache.getOrCreateTypeInfo(fieldDescriptor);
    }

    /**
     * Converts a Protobuf field to a MaxCompute record field.
     *
     * @param fieldDescriptor the Protobuf field descriptor
     * @param parsedObject parsed Protobuf field
     * @return the MaxCompute record field
     */
    public Object toMaxComputeValue(Descriptors.FieldDescriptor fieldDescriptor, Object parsedObject) {
        ProtobufMaxComputeConverter protobufMaxComputeConverter = maxComputeProtobufConverterCache.getConverter(fieldDescriptor);
        return protobufMaxComputeConverter.convertPayload(new ProtoPayload(fieldDescriptor, parsedObject, true));
    }

    /**
     * Clears the cache. This method should be called when the schema changes.
     */
    public void clearCache() {
        maxComputeProtobufConverterCache.clearCache();
    }

}
