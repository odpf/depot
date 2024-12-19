package com.gotocompany.depot.maxcompute.converter;

import com.aliyun.odps.data.SimpleStruct;
import com.aliyun.odps.type.StructTypeInfo;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.gotocompany.depot.maxcompute.model.ProtoPayload;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Converts google.protobuf.Duration to MaxCompute Struct.
 */
public class DurationProtobufMaxComputeConverter implements ProtobufMaxComputeConverter {

    private static final String SECONDS = "seconds";
    private static final String NANOS = "nanos";
    private static final List<String> FIELD_NAMES = Arrays.asList(SECONDS, NANOS);
    private static final List<TypeInfo> TYPE_INFOS = Arrays.asList(TypeInfoFactory.BIGINT, TypeInfoFactory.INT);

    @Override
    public TypeInfo convertSingularTypeInfo(Descriptors.FieldDescriptor fieldDescriptor) {
        return TypeInfoFactory.getStructTypeInfo(FIELD_NAMES, TYPE_INFOS);
    }

    @Override
    public Object convertSingularPayload(ProtoPayload protoPayload) {
        List<Object> values = getValues((Message) protoPayload.getParsedObject());
        return new SimpleStruct((StructTypeInfo) convertSingularTypeInfo(protoPayload.getFieldDescriptor()), values);
    }

    private static List<Object> getValues(Message durationMessage) {
        List<Object> values = new ArrayList<>();
        values.add(durationMessage.getField(durationMessage.getDescriptorForType().findFieldByName(SECONDS)));
        values.add(durationMessage.getField(durationMessage.getDescriptorForType().findFieldByName(NANOS)));
        return values;
    }

}
