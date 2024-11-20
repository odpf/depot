package com.gotocompany.depot.maxcompute.converter;

import com.aliyun.odps.type.TypeInfo;
import com.google.protobuf.Descriptors;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.maxcompute.converter.payload.DurationPayloadConverter;
import com.gotocompany.depot.maxcompute.converter.payload.MessagePayloadConverter;
import com.gotocompany.depot.maxcompute.converter.payload.PayloadConverter;
import com.gotocompany.depot.maxcompute.converter.payload.PrimitivePayloadConverter;
import com.gotocompany.depot.maxcompute.converter.payload.StructPayloadConverter;
import com.gotocompany.depot.maxcompute.converter.payload.TimestampPayloadConverter;
import com.gotocompany.depot.maxcompute.converter.type.DurationTypeInfoConverter;
import com.gotocompany.depot.maxcompute.converter.type.MessageTypeInfoConverter;
import com.gotocompany.depot.maxcompute.converter.type.PrimitiveTypeInfoConverter;
import com.gotocompany.depot.maxcompute.converter.type.StructTypeInfoConverter;
import com.gotocompany.depot.maxcompute.converter.type.TimestampTypeInfoConverter;
import com.gotocompany.depot.maxcompute.converter.type.TypeInfoConverter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ConverterOrchestrator {

    private final List<TypeInfoConverter> typeInfoConverters;
    private final List<PayloadConverter> payloadConverters;
    private final Map<String, TypeInfo> typeInfoCache;

    public ConverterOrchestrator(MaxComputeSinkConfig maxComputeSinkConfig) {
        typeInfoConverters = new ArrayList<>();
        payloadConverters = new ArrayList<>();
        typeInfoCache = new ConcurrentHashMap<>();
        initializeConverters(maxComputeSinkConfig);
    }

    public TypeInfo convert(Descriptors.FieldDescriptor fieldDescriptor) {
        return typeInfoCache.computeIfAbsent(fieldDescriptor.getFullName(), key -> typeInfoConverters.stream()
                .filter(converter -> converter.canConvert(fieldDescriptor))
                .findFirst()
                .map(converter -> converter.convert(fieldDescriptor))
                .orElseThrow(() -> new IllegalArgumentException("Unsupported type: " + fieldDescriptor.getType())));
    }

    public Object convert(Descriptors.FieldDescriptor fieldDescriptor, Object object) {
        return payloadConverters.stream()
                .filter(converter -> converter.canConvert(fieldDescriptor))
                .findFirst()
                .map(converter -> converter.convert(fieldDescriptor, object))
                .orElseThrow(() -> new IllegalArgumentException("Unsupported type: " + fieldDescriptor.getType()));
    }

    public void clearCache() {
        typeInfoCache.clear();
    }

    private void initializeConverters(MaxComputeSinkConfig maxComputeSinkConfig) {
        PrimitiveTypeInfoConverter primitiveTypeInfoConverter = new PrimitiveTypeInfoConverter();
        DurationTypeInfoConverter durationTypeInfoConverter = new DurationTypeInfoConverter();
        StructTypeInfoConverter structTypeInfoConverter = new StructTypeInfoConverter();
        TimestampTypeInfoConverter timestampTypeInfoConverter = new TimestampTypeInfoConverter();
        MessageTypeInfoConverter messageTypeInfoConverter = new MessageTypeInfoConverter(typeInfoConverters);

        typeInfoConverters.add(primitiveTypeInfoConverter);
        typeInfoConverters.add(durationTypeInfoConverter);
        typeInfoConverters.add(structTypeInfoConverter);
        typeInfoConverters.add(timestampTypeInfoConverter);
        typeInfoConverters.add(messageTypeInfoConverter);

        payloadConverters.add(new PrimitivePayloadConverter(primitiveTypeInfoConverter));
        payloadConverters.add(new DurationPayloadConverter(durationTypeInfoConverter));
        payloadConverters.add(new StructPayloadConverter(structTypeInfoConverter));
        payloadConverters.add(new TimestampPayloadConverter(timestampTypeInfoConverter, maxComputeSinkConfig));
        payloadConverters.add(new MessagePayloadConverter(messageTypeInfoConverter, payloadConverters));
    }

}
