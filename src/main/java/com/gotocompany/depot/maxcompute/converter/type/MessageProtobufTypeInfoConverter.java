package com.gotocompany.depot.maxcompute.converter.type;

import com.aliyun.odps.type.StructTypeInfo;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.protobuf.Descriptors;

import java.util.List;
import java.util.stream.Collectors;

public class MessageProtobufTypeInfoConverter implements ProtobufTypeInfoConverter {

    private final List<ProtobufTypeInfoConverter> protoFieldToProtobufTypeInfoConverterList;

    public MessageProtobufTypeInfoConverter(List<ProtobufTypeInfoConverter> protoFieldToProtobufTypeInfoConverterList) {
        this.protoFieldToProtobufTypeInfoConverterList = protoFieldToProtobufTypeInfoConverterList;
    }

    @Override
    public StructTypeInfo convertSingular(Descriptors.FieldDescriptor fieldDescriptor) {
        List<String> fieldNames = fieldDescriptor.getMessageType().getFields().stream()
                .map(Descriptors.FieldDescriptor::getName)
                .collect(Collectors.toList());
        List<TypeInfo> typeInfos = fieldDescriptor.getMessageType().getFields().stream()
                .map(fd -> protoFieldToProtobufTypeInfoConverterList.stream()
                        .filter(converter -> converter.canConvert(fd))
                        .findFirst()
                        .map(converter -> converter.convert(fd))
                        .orElseThrow(() -> new IllegalArgumentException("Unsupported type: " + fd.getJavaType())))
                .collect(Collectors.toList());
        return TypeInfoFactory.getStructTypeInfo(fieldNames, typeInfos);
    }

    @Override
    public boolean canConvert(Descriptors.FieldDescriptor fieldDescriptor) {
        return Descriptors.FieldDescriptor.Type.MESSAGE.equals(fieldDescriptor.getType());
    }

}
