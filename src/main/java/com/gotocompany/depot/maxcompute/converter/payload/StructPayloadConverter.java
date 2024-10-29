package com.gotocompany.depot.maxcompute.converter.payload;

import com.google.protobuf.Descriptors;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import com.gotocompany.depot.maxcompute.converter.type.StructTypeInfoConverter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class StructPayloadConverter implements PayloadConverter {

    private final StructTypeInfoConverter structTypeInfoConverter;
    private final JsonFormat.Printer printer = JsonFormat.printer()
            .preservingProtoFieldNames()
            .omittingInsignificantWhitespace();

    @Override
    public Object convertSingular(Descriptors.FieldDescriptor fieldDescriptor, Object object) {
        try {
            return printer.print((Message) object);
        } catch (InvalidProtocolBufferException e) {
            return "";
        }
    }

    @Override
    public boolean canConvert(Descriptors.FieldDescriptor fieldDescriptor) {
        return structTypeInfoConverter.canConvert(fieldDescriptor);
    }

}
