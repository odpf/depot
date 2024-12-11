package com.gotocompany.depot.maxcompute.converter;

import com.aliyun.odps.data.SimpleStruct;
import com.aliyun.odps.type.StructTypeInfo;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.gotocompany.depot.maxcompute.model.MaxComputeProtobufConverterCache;
import com.gotocompany.depot.maxcompute.model.ProtoPayload;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Setter
public class MessageProtobufMaxComputeConverter implements ProtobufMaxComputeConverter {

    private final MaxComputeProtobufConverterCache maxComputeProtobufConverterCache;

    public MessageProtobufMaxComputeConverter(MaxComputeProtobufConverterCache maxComputeProtobufConverterCache) {
        this.maxComputeProtobufConverterCache = maxComputeProtobufConverterCache;
    }

    @Override
    public TypeInfo convertTypeInfo(Descriptors.FieldDescriptor fieldDescriptor) {
        return maxComputeProtobufConverterCache.getOrCreateTypeInfo(fieldDescriptor,
                () -> wrapTypeInfo(fieldDescriptor, convertSingularTypeInfo(fieldDescriptor)));
    }

    @Override
    public StructTypeInfo convertSingularTypeInfo(Descriptors.FieldDescriptor fieldDescriptor) {
        List<String> fieldNames = fieldDescriptor.getMessageType().getFields().stream()
                .map(Descriptors.FieldDescriptor::getName)
                .collect(Collectors.toList());
        List<TypeInfo> typeInfos = fieldDescriptor.getMessageType().getFields().stream()
                .map(fd -> {
                    ProtobufMaxComputeConverter converter = maxComputeProtobufConverterCache.getConverter(fd);
                    return converter.convertTypeInfo(fd);
                })
                .collect(Collectors.toList());
        return TypeInfoFactory.getStructTypeInfo(fieldNames, typeInfos);
    }

    @Override
    public boolean canConvert(Descriptors.FieldDescriptor fieldDescriptor) {
        return Descriptors.FieldDescriptor.Type.MESSAGE.equals(fieldDescriptor.getType());
    }

    @Override
    public Object convertSingularPayload(ProtoPayload protoPayload) {
        Message dynamicMessage = (Message) protoPayload.getParsedObject();
        List<Object> values = new ArrayList<>();
        Map<Descriptors.FieldDescriptor, Object> payloadFields = dynamicMessage.getAllFields();
        protoPayload.getFieldDescriptor().getMessageType().getFields().forEach(innerFieldDescriptor -> {
            if (!payloadFields.containsKey(innerFieldDescriptor)) {
                values.add(null);
                return;
            }
            Object mappedInnerValue = maxComputeProtobufConverterCache.getConverter(innerFieldDescriptor)
                    .convertPayload(new ProtoPayload(innerFieldDescriptor, payloadFields.get(innerFieldDescriptor), false));
            values.add(mappedInnerValue);
        });
        return new SimpleStruct(convertSingularTypeInfo(protoPayload.getFieldDescriptor()), values);
    }

}
