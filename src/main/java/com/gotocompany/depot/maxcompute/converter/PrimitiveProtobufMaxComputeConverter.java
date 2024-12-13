package com.gotocompany.depot.maxcompute.converter;

import com.aliyun.odps.data.Binary;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.gotocompany.depot.exception.InvalidMessageException;
import com.gotocompany.depot.maxcompute.model.ProtoPayload;

import java.util.Map;
import java.util.function.Function;

import static com.google.protobuf.Descriptors.FieldDescriptor.Type.*;

/**
 * Handle the conversion of primitive protobuf types to MaxCompute compatible format.
 */
public class PrimitiveProtobufMaxComputeConverter implements ProtobufMaxComputeConverter {

    private static final Map<Descriptors.FieldDescriptor.Type, TypeInfo> PROTO_TYPE_MAP;
    private static final Map<Descriptors.FieldDescriptor.Type, Function<Object, Object>> PROTO_PAYLOAD_MAPPER_MAP;

    static {
        PROTO_TYPE_MAP = ImmutableMap.<Descriptors.FieldDescriptor.Type, TypeInfo>builder()
                .put(BYTES, TypeInfoFactory.BINARY)
                .put(STRING, TypeInfoFactory.STRING)
                .put(ENUM, TypeInfoFactory.STRING)
                .put(DOUBLE, TypeInfoFactory.DOUBLE)
                .put(FLOAT, TypeInfoFactory.FLOAT)
                .put(BOOL, TypeInfoFactory.BOOLEAN)
                .put(INT64, TypeInfoFactory.BIGINT)
                .put(UINT64, TypeInfoFactory.BIGINT)
                .put(INT32, TypeInfoFactory.INT)
                .put(UINT32, TypeInfoFactory.INT)
                .put(FIXED64, TypeInfoFactory.BIGINT)
                .put(FIXED32, TypeInfoFactory.INT)
                .put(SFIXED32, TypeInfoFactory.INT)
                .put(SFIXED64, TypeInfoFactory.BIGINT)
                .put(SINT32, TypeInfoFactory.INT)
                .put(SINT64, TypeInfoFactory.BIGINT)
                .build();

        PROTO_PAYLOAD_MAPPER_MAP = ImmutableMap.<Descriptors.FieldDescriptor.Type, Function<Object, Object>>builder()
                .put(BYTES, object -> handleBytes((ByteString) object))
                .put(ENUM, Object::toString)
                .put(FLOAT, object -> handleFloat((float) object))
                .put(DOUBLE, object -> handleDouble((double) object))
                .build();
    }

    @Override
    public TypeInfo convertSingularTypeInfo(Descriptors.FieldDescriptor fieldDescriptor) {
        return PROTO_TYPE_MAP.get(fieldDescriptor.getType());
    }

    @Override
    public boolean canConvert(Descriptors.FieldDescriptor fieldDescriptor) {
        return PROTO_TYPE_MAP.containsKey(fieldDescriptor.getType());
    }

    @Override
    public Object convertSingularPayload(ProtoPayload protoPayload) {
        return PROTO_PAYLOAD_MAPPER_MAP.getOrDefault(protoPayload.getFieldDescriptor().getType(), Function.identity())
                .apply(protoPayload.getParsedObject());
    }

    private static double handleDouble(double value) {
        if (!Double.isFinite(value)) {
            throw new InvalidMessageException("Invalid float value: " + value);
        }
        return value;
    }

    private static float handleFloat(float value) {
        if (!Float.isFinite(value)) {
            throw new InvalidMessageException("Invalid float value: " + value);
        }
        return value;
    }

    private static Binary handleBytes(ByteString object) {
        return new Binary(object.toByteArray());
    }

}
