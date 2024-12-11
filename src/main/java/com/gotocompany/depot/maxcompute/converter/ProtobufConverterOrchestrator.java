package com.gotocompany.depot.maxcompute.converter;

import com.aliyun.odps.type.TypeInfo;
import com.google.protobuf.Descriptors;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.maxcompute.model.MaxComputeProtobufConverterCache;
import com.gotocompany.depot.maxcompute.model.ProtoPayload;

public class ProtobufConverterOrchestrator {

    private final MaxComputeProtobufConverterCache maxComputeProtobufConverterCache;

    public ProtobufConverterOrchestrator(MaxComputeSinkConfig maxComputeSinkConfig) {
        maxComputeProtobufConverterCache = new MaxComputeProtobufConverterCache(maxComputeSinkConfig);
    }

    public TypeInfo toMaxComputeTypeInfo(Descriptors.FieldDescriptor fieldDescriptor) {
        return maxComputeProtobufConverterCache.getOrCreateTypeInfo(fieldDescriptor);
    }

    public Object toMaxComputeValue(Descriptors.FieldDescriptor fieldDescriptor, Object object) {
        ProtobufMaxComputeConverter protobufMaxComputeConverter = maxComputeProtobufConverterCache.getConverter(fieldDescriptor);
        return protobufMaxComputeConverter.convertPayload(new ProtoPayload(fieldDescriptor, object, true));
    }

    public void clearCache() {
        maxComputeProtobufConverterCache.clearCache();
    }

}
