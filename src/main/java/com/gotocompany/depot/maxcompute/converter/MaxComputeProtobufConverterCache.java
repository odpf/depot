package com.gotocompany.depot.maxcompute.converter;

import com.aliyun.odps.type.TypeInfo;
import com.google.common.collect.Sets;
import com.google.protobuf.Descriptors;
import com.gotocompany.depot.config.MaxComputeSinkConfig;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import static com.google.protobuf.Descriptors.FieldDescriptor.Type.*;

public class MaxComputeProtobufConverterCache {

    private static final String GOOGLE_PROTOBUF_TIMESTAMP = "google.protobuf.Timestamp";
    private static final String GOOGLE_PROTOBUF_DURATION = "google.protobuf.Duration";
    private static final String GOOGLE_PROTOBUF_STRUCT = "google.protobuf.Struct";
    private static final Set<String> SUPPORTED_PRIMITIVE_PROTO_TYPES = Sets.newHashSet(
            BYTES.toString(), STRING.toString(), ENUM.toString(), DOUBLE.toString(), FLOAT.toString(),
            BOOL.toString(), INT64.toString(), UINT64.toString(), INT32.toString(), UINT32.toString(),
            FIXED64.toString(), FIXED32.toString(), SFIXED32.toString(), SFIXED64.toString(),
            SINT32.toString(), SINT64.toString());

    private final Map<String, ProtobufMaxComputeConverter> protobufMaxComputeConverterMap;
    private final Map<String, TypeInfo> typeInfoCache;
    private final MaxComputeSinkConfig maxComputeSinkConfig;

    public MaxComputeProtobufConverterCache(MaxComputeSinkConfig maxComputeSinkConfig) {
        this.protobufMaxComputeConverterMap = new ConcurrentHashMap<>();
        this.typeInfoCache = new ConcurrentHashMap<>();
        this.maxComputeSinkConfig = maxComputeSinkConfig;
        initMaxComputeConverterMap();
    }

    public TypeInfo getOrCreateTypeInfo(Descriptors.FieldDescriptor fieldDescriptor) {
        if (typeInfoCache.containsKey(fieldDescriptor.getFullName())) {
            return typeInfoCache.get(fieldDescriptor.getFullName());
        }
        ProtobufMaxComputeConverter protobufMaxComputeConverter = getConverter(fieldDescriptor);
        TypeInfo typeInfo = protobufMaxComputeConverter.convertTypeInfo(fieldDescriptor);
        typeInfoCache.put(fieldDescriptor.getFullName(), typeInfo);
        return typeInfo;
    }

    public TypeInfo getOrCreateTypeInfo(Descriptors.FieldDescriptor fieldDescriptor, Supplier<TypeInfo> supplier) {
        if (typeInfoCache.containsKey(fieldDescriptor.getFullName())) {
            return typeInfoCache.get(fieldDescriptor.getFullName());
        }
        TypeInfo typeInfo = supplier.get();
        typeInfoCache.put(fieldDescriptor.getFullName(), typeInfo);
        return typeInfo;
    }

    public ProtobufMaxComputeConverter getConverter(Descriptors.FieldDescriptor fieldDescriptor) {
        ProtobufMaxComputeConverter protobufMaxComputeConverter = null;
        if (fieldDescriptor.getType().equals(MESSAGE)) {
            protobufMaxComputeConverter = protobufMaxComputeConverterMap.get(fieldDescriptor.getMessageType().getFullName());
        }
        protobufMaxComputeConverter = protobufMaxComputeConverter != null ? protobufMaxComputeConverter
                : protobufMaxComputeConverterMap.get(fieldDescriptor.getType().toString());
        if (protobufMaxComputeConverter == null) {
            throw new IllegalArgumentException("Unsupported type: " + fieldDescriptor.getType());
        }
        return protobufMaxComputeConverter;
    }

    public void clearCache() {
        typeInfoCache.clear();
    }

    private void initMaxComputeConverterMap() {
        PrimitiveProtobufMaxComputeConverter primitiveProtobufMaxComputeConverter =
                new PrimitiveProtobufMaxComputeConverter();
        SUPPORTED_PRIMITIVE_PROTO_TYPES.forEach(type -> protobufMaxComputeConverterMap.put(type, primitiveProtobufMaxComputeConverter));
        protobufMaxComputeConverterMap.put(GOOGLE_PROTOBUF_TIMESTAMP, new TimestampProtobufMaxComputeConverter(maxComputeSinkConfig));
        protobufMaxComputeConverterMap.put(GOOGLE_PROTOBUF_DURATION, new DurationProtobufMaxComputeConverter());
        protobufMaxComputeConverterMap.put(GOOGLE_PROTOBUF_STRUCT, new StructProtobufMaxComputeConverter());
        protobufMaxComputeConverterMap.put(MESSAGE.toString(), new MessageProtobufMaxComputeConverter(this));
    }

}
