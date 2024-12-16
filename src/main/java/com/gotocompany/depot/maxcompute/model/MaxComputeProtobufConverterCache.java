package com.gotocompany.depot.maxcompute.model;

import com.aliyun.odps.type.TypeInfo;
import com.google.protobuf.Descriptors;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.maxcompute.converter.DurationProtobufMaxComputeConverter;
import com.gotocompany.depot.maxcompute.converter.MessageProtobufMaxComputeConverter;
import com.gotocompany.depot.maxcompute.converter.PrimitiveProtobufMaxComputeConverter;
import com.gotocompany.depot.maxcompute.converter.ProtobufMaxComputeConverter;
import com.gotocompany.depot.maxcompute.converter.StructProtobufMaxComputeConverter;
import com.gotocompany.depot.maxcompute.converter.TimestampProtobufMaxComputeConverter;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import static com.google.protobuf.Descriptors.FieldDescriptor.Type.*;

public class MaxComputeProtobufConverterCache {
    public static final String GOOGLE_PROTOBUF_TIMESTAMP = "google.protobuf.Timestamp";
    public static final String GOOGLE_PROTOBUF_DURATION = "google.protobuf.Duration";
    public static final String GOOGLE_PROTOBUF_STRUCT = "google.protobuf.Struct";
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
        protobufMaxComputeConverterMap.put(BYTES.toString(), primitiveProtobufMaxComputeConverter);
        protobufMaxComputeConverterMap.put(STRING.toString(), primitiveProtobufMaxComputeConverter);
        protobufMaxComputeConverterMap.put(ENUM.toString(), primitiveProtobufMaxComputeConverter);
        protobufMaxComputeConverterMap.put(DOUBLE.toString(), primitiveProtobufMaxComputeConverter);
        protobufMaxComputeConverterMap.put(FLOAT.toString(), primitiveProtobufMaxComputeConverter);
        protobufMaxComputeConverterMap.put(BOOL.toString(), primitiveProtobufMaxComputeConverter);
        protobufMaxComputeConverterMap.put(INT64.toString(), primitiveProtobufMaxComputeConverter);
        protobufMaxComputeConverterMap.put(UINT64.toString(), primitiveProtobufMaxComputeConverter);
        protobufMaxComputeConverterMap.put(INT32.toString(), primitiveProtobufMaxComputeConverter);
        protobufMaxComputeConverterMap.put(UINT32.toString(), primitiveProtobufMaxComputeConverter);
        protobufMaxComputeConverterMap.put(FIXED64.toString(), primitiveProtobufMaxComputeConverter);
        protobufMaxComputeConverterMap.put(FIXED32.toString(), primitiveProtobufMaxComputeConverter);
        protobufMaxComputeConverterMap.put(SFIXED32.toString(), primitiveProtobufMaxComputeConverter);
        protobufMaxComputeConverterMap.put(SFIXED64.toString(), primitiveProtobufMaxComputeConverter);
        protobufMaxComputeConverterMap.put(SINT32.toString(), primitiveProtobufMaxComputeConverter);
        protobufMaxComputeConverterMap.put(SINT64.toString(), primitiveProtobufMaxComputeConverter);
        protobufMaxComputeConverterMap.put(GOOGLE_PROTOBUF_TIMESTAMP, new TimestampProtobufMaxComputeConverter(maxComputeSinkConfig));
        protobufMaxComputeConverterMap.put(GOOGLE_PROTOBUF_DURATION, new DurationProtobufMaxComputeConverter());
        protobufMaxComputeConverterMap.put(GOOGLE_PROTOBUF_STRUCT, new StructProtobufMaxComputeConverter());
        MessageProtobufMaxComputeConverter messageProtobufMaxComputeConverter = new MessageProtobufMaxComputeConverter(this);
        protobufMaxComputeConverterMap.put(MESSAGE.toString(), messageProtobufMaxComputeConverter);
    }

}
