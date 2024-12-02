package com.gotocompany.depot.maxcompute.converter.payload;

import com.google.protobuf.Descriptors;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import com.gotocompany.depot.maxcompute.converter.type.StructProtobufTypeInfoConverter;
import com.gotocompany.depot.maxcompute.model.ProtoPayload;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class StructProtobufPayloadConverter implements ProtobufPayloadConverter {

    private final StructProtobufTypeInfoConverter structTypeInfoConverter;
    private final JsonFormat.Printer printer = JsonFormat.printer()
            .preservingProtoFieldNames()
            .omittingInsignificantWhitespace();

    @Override
    public Object convertSingular(ProtoPayload protoPayload) {
        try {
            return printer.print((Message) protoPayload.getObject());
        } catch (InvalidProtocolBufferException e) {
            return "";
        }
    }

    @Override
    public boolean canConvert(Descriptors.FieldDescriptor fieldDescriptor) {
        return structTypeInfoConverter.canConvert(fieldDescriptor);
    }

}
