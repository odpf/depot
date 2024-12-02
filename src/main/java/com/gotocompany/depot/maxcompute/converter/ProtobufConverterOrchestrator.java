package com.gotocompany.depot.maxcompute.converter;

import com.aliyun.odps.type.TypeInfo;
import com.google.protobuf.Descriptors;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.maxcompute.converter.payload.DurationProtobufPayloadConverter;
import com.gotocompany.depot.maxcompute.converter.payload.MessageProtobufPayloadConverter;
import com.gotocompany.depot.maxcompute.converter.payload.ProtobufPayloadConverter;
import com.gotocompany.depot.maxcompute.converter.payload.PrimitiveProtobufPayloadConverter;
import com.gotocompany.depot.maxcompute.converter.payload.StructProtobufPayloadConverter;
import com.gotocompany.depot.maxcompute.converter.payload.TimestampProtobufPayloadConverter;
import com.gotocompany.depot.maxcompute.converter.type.DurationProtobufTypeInfoConverter;
import com.gotocompany.depot.maxcompute.converter.type.MessageProtobufTypeInfoConverter;
import com.gotocompany.depot.maxcompute.converter.type.PrimitiveProtobufTypeInfoConverter;
import com.gotocompany.depot.maxcompute.converter.type.StructProtobufTypeInfoConverter;
import com.gotocompany.depot.maxcompute.converter.type.TimestampProtobufTypeInfoConverter;
import com.gotocompany.depot.maxcompute.converter.type.ProtobufTypeInfoConverter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ProtobufConverterOrchestrator {

    private final List<ProtobufTypeInfoConverter> protobufTypeInfoConverters;
    private final List<ProtobufPayloadConverter> protobufPayloadConverters;
    private final Map<String, TypeInfo> typeInfoCache;

    public ProtobufConverterOrchestrator(MaxComputeSinkConfig maxComputeSinkConfig) {
        protobufTypeInfoConverters = new ArrayList<>();
        protobufPayloadConverters = new ArrayList<>();
        typeInfoCache = new ConcurrentHashMap<>();
        initializeConverters(maxComputeSinkConfig);
    }

    public TypeInfo convert(Descriptors.FieldDescriptor fieldDescriptor) {
        return typeInfoCache.computeIfAbsent(fieldDescriptor.getFullName(), key -> protobufTypeInfoConverters.stream()
                .filter(converter -> converter.canConvert(fieldDescriptor))
                .findFirst()
                .map(converter -> converter.convert(fieldDescriptor))
                .orElseThrow(() -> new IllegalArgumentException("Unsupported type: " + fieldDescriptor.getType())));
    }

    public Object convert(Descriptors.FieldDescriptor fieldDescriptor, Object object) {
        return protobufPayloadConverters.stream()
                .filter(converter -> converter.canConvert(fieldDescriptor))
                .findFirst()
                .map(converter -> converter.convert(fieldDescriptor, object))
                .orElseThrow(() -> new IllegalArgumentException("Unsupported type: " + fieldDescriptor.getType()));
    }

    public void clearCache() {
        typeInfoCache.clear();
    }

    private void initializeConverters(MaxComputeSinkConfig maxComputeSinkConfig) {
        PrimitiveProtobufTypeInfoConverter primitiveTypeInfoConverter = new PrimitiveProtobufTypeInfoConverter();
        DurationProtobufTypeInfoConverter durationTypeInfoConverter = new DurationProtobufTypeInfoConverter();
        StructProtobufTypeInfoConverter structTypeInfoConverter = new StructProtobufTypeInfoConverter();
        TimestampProtobufTypeInfoConverter timestampTypeInfoConverter = new TimestampProtobufTypeInfoConverter();
        MessageProtobufTypeInfoConverter messageTypeInfoConverter = new MessageProtobufTypeInfoConverter(protobufTypeInfoConverters);

        protobufTypeInfoConverters.add(primitiveTypeInfoConverter);
        protobufTypeInfoConverters.add(durationTypeInfoConverter);
        protobufTypeInfoConverters.add(structTypeInfoConverter);
        protobufTypeInfoConverters.add(timestampTypeInfoConverter);
        protobufTypeInfoConverters.add(messageTypeInfoConverter);

        protobufPayloadConverters.add(new PrimitiveProtobufPayloadConverter(primitiveTypeInfoConverter));
        protobufPayloadConverters.add(new DurationProtobufPayloadConverter(durationTypeInfoConverter));
        protobufPayloadConverters.add(new StructProtobufPayloadConverter(structTypeInfoConverter));
        protobufPayloadConverters.add(new TimestampProtobufPayloadConverter(timestampTypeInfoConverter, maxComputeSinkConfig));
        protobufPayloadConverters.add(new MessageProtobufPayloadConverter(messageTypeInfoConverter, protobufPayloadConverters));
    }

}
