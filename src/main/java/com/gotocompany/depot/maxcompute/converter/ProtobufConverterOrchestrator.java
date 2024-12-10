package com.gotocompany.depot.maxcompute.converter;

import com.aliyun.odps.type.TypeInfo;
import com.google.protobuf.Descriptors;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.maxcompute.model.ProtoPayload;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ProtobufConverterOrchestrator {

    private final List<ProtobufMaxComputeConverter> protobufMaxComputeConverters;
    private final Map<String, TypeInfo> typeInfoCache;

    public ProtobufConverterOrchestrator(MaxComputeSinkConfig maxComputeSinkConfig) {
        protobufMaxComputeConverters = new ArrayList<>();
        typeInfoCache = new ConcurrentHashMap<>();
        initializeConverters(maxComputeSinkConfig);
    }

    public TypeInfo toMaxComputeTypeInfo(Descriptors.FieldDescriptor fieldDescriptor) {
        return typeInfoCache.computeIfAbsent(fieldDescriptor.getFullName(), key -> protobufMaxComputeConverters.stream()
                .filter(converter -> converter.canConvert(fieldDescriptor))
                .findFirst()
                .map(converter -> converter.convertTypeInfo(fieldDescriptor))
                .orElseThrow(() -> new IllegalArgumentException("Unsupported type: " + fieldDescriptor.getType())));
    }

    public Object toMaxComputeValue(Descriptors.FieldDescriptor fieldDescriptor, Object object) {
        return protobufMaxComputeConverters.stream()
                .filter(converter -> converter.canConvert(fieldDescriptor))
                .findFirst()
                .map(converter -> converter.convertPayload(new ProtoPayload(fieldDescriptor, object, true)))
                .orElseThrow(() -> new IllegalArgumentException("Unsupported type: " + fieldDescriptor.getType()));
    }

    public void clearCache() {
        typeInfoCache.clear();
    }

    private void initializeConverters(MaxComputeSinkConfig maxComputeSinkConfig) {
        PrimitiveProtobufMaxComputeConverter primitiveMaxComputeConverter = new PrimitiveProtobufMaxComputeConverter();
        DurationProtobufMaxComputeConverter durationMaxComputeConverter = new DurationProtobufMaxComputeConverter();
        StructProtobufMaxComputeConverter structMaxComputeConverter = new StructProtobufMaxComputeConverter();
        TimestampProtobufMaxComputeConverter timestampMaxComputeConverter = new TimestampProtobufMaxComputeConverter(maxComputeSinkConfig);
        MessageProtobufMaxComputeConverter messageMaxComputeConverter = new MessageProtobufMaxComputeConverter(protobufMaxComputeConverters);

        protobufMaxComputeConverters.add(primitiveMaxComputeConverter);
        protobufMaxComputeConverters.add(durationMaxComputeConverter);
        protobufMaxComputeConverters.add(structMaxComputeConverter);
        protobufMaxComputeConverters.add(timestampMaxComputeConverter);
        protobufMaxComputeConverters.add(messageMaxComputeConverter);
    }

}
