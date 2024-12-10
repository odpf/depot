package com.gotocompany.depot.maxcompute.converter;

import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.protobuf.Descriptors;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import com.gotocompany.depot.maxcompute.model.ProtoPayload;

public class StructProtobufMaxComputeConverter implements ProtobufMaxComputeConverter {

    private static final String GOOGLE_PROTOBUF_STRUCT = "google.protobuf.Struct";
    private final JsonFormat.Printer printer = JsonFormat.printer()
            .preservingProtoFieldNames()
            .omittingInsignificantWhitespace();

    @Override
    public TypeInfo convertSingularTypeInfo(Descriptors.FieldDescriptor fieldDescriptor) {
        return TypeInfoFactory.STRING;
    }

    @Override
    public boolean canConvert(Descriptors.FieldDescriptor fieldDescriptor) {
        return fieldDescriptor.getType() == Descriptors.FieldDescriptor.Type.MESSAGE
                && fieldDescriptor.getMessageType().getFullName().equals(GOOGLE_PROTOBUF_STRUCT);
    }

    @Override
    public Object convertSingularPayload(ProtoPayload protoPayload) {
        try {
            return printer.print((Message) protoPayload.getParsedObject());
        } catch (InvalidProtocolBufferException e) {
            return "";
        }
    }

}
