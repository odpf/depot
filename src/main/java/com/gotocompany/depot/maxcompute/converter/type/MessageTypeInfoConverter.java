package com.gotocompany.depot.maxcompute.converter.type;

import com.aliyun.odps.type.StructTypeInfo;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.protobuf.Descriptors;

import java.util.List;
import java.util.stream.Collectors;

public class MessageTypeInfoConverter implements TypeInfoConverter {

    private final List<TypeInfoConverter> protoFieldToTypeInfoConverterList;

    public MessageTypeInfoConverter(List<TypeInfoConverter> protoFieldToTypeInfoConverterList) {
        this.protoFieldToTypeInfoConverterList = protoFieldToTypeInfoConverterList;
    }

    @Override
    public StructTypeInfo convertSingular(Descriptors.FieldDescriptor fieldDescriptor) {
        List<String> fieldNames = fieldDescriptor.getMessageType().getFields().stream()
                .map(Descriptors.FieldDescriptor::getName)
                .collect(Collectors.toList());
        List<TypeInfo> typeInfos = fieldDescriptor.getMessageType().getFields().stream()
                .map(fd -> protoFieldToTypeInfoConverterList.stream()
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
