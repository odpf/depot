package com.gotocompany.depot.maxcompute.converter;

import com.aliyun.odps.data.SimpleStruct;
import com.aliyun.odps.type.StructTypeInfo;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.gotocompany.depot.maxcompute.model.ProtoPayload;
import lombok.RequiredArgsConstructor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@RequiredArgsConstructor
public class MessageProtobufMaxComputeConverter implements ProtobufMaxComputeConverter {

    private final List<ProtobufMaxComputeConverter> protobufMaxComputeConverters;

    @Override
    public StructTypeInfo convertSingularTypeInfo(Descriptors.FieldDescriptor fieldDescriptor) {
        List<String> fieldNames = fieldDescriptor.getMessageType().getFields().stream()
                .map(Descriptors.FieldDescriptor::getName)
                .collect(Collectors.toList());
        List<TypeInfo> typeInfos = fieldDescriptor.getMessageType().getFields().stream()
                .map(fd -> protobufMaxComputeConverters.stream()
                        .filter(converter -> converter.canConvert(fd))
                        .findFirst()
                        .map(converter -> converter.convertTypeInfo(fd))
                        .orElseThrow(() -> new IllegalArgumentException("Unsupported type: " + fd.getJavaType())))
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
            Object mappedInnerValue = protobufMaxComputeConverters.stream()
                    .filter(converter -> converter.canConvert(innerFieldDescriptor))
                    .findFirst()
                    .map(converter -> converter.convertPayload(new ProtoPayload(innerFieldDescriptor, payloadFields.get(innerFieldDescriptor), false)))
                    .orElse(null);
            values.add(mappedInnerValue);
        });
        return new SimpleStruct(convertSingularTypeInfo(protoPayload.getFieldDescriptor()), values);
    }

}
