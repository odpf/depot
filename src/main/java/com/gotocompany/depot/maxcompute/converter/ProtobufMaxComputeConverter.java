package com.gotocompany.depot.maxcompute.converter;

import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.protobuf.Descriptors;
import com.gotocompany.depot.maxcompute.model.ProtoPayload;

import java.util.List;
import java.util.stream.Collectors;

public interface ProtobufMaxComputeConverter {

    /**
     * Converts a Protobuf field descriptor to a MaxCompute TypeInfo.
     * This method wraps the singular type conversion with array type handling if the field is repeated.
     *
     * @param fieldDescriptor the Protobuf field descriptor to convert
     * @return the corresponding MaxCompute TypeInfo
     */
    default TypeInfo convertTypeInfo(Descriptors.FieldDescriptor fieldDescriptor) {
        TypeInfo typeInfo = convertSingularTypeInfo(fieldDescriptor);
        return fieldDescriptor.isRepeated() ? TypeInfoFactory.getArrayTypeInfo(typeInfo) : typeInfo;
    }

    /**
     * Converts a singular Protobuf field descriptor to a MaxCompute TypeInfo.
     * This method should be implemented by subclasses to handle specific field types.
     *
     * @param fieldDescriptor the Protobuf field descriptor to convert
     * @return the corresponding MaxCompute TypeInfo for the singular field
     */
    TypeInfo convertSingularTypeInfo(Descriptors.FieldDescriptor fieldDescriptor);

    /**
     * Checks if the converter can handle the given Protobuf field descriptor.
     *
     * @param fieldDescriptor the Protobuf field descriptor to check
     * @return true if the converter can handle the field descriptor, false otherwise
     */
    boolean canConvert(Descriptors.FieldDescriptor fieldDescriptor);

    /**
     * Converts a proto payload to a format that can be used by the MaxCompute SDK.
     * @param protoPayload the proto payload to convert, containing field descriptor, the actual object and level
     * @return the converted object
     */
    default Object convertPayload(ProtoPayload protoPayload) {
        if (!protoPayload.getFieldDescriptor().isRepeated()) {
            return convertSingularPayload(protoPayload);
        }
        return ((List<?>) protoPayload.getParsedObject()).stream()
                .map(o -> convertSingularPayload(new ProtoPayload(protoPayload.getFieldDescriptor(), o, protoPayload.isRootLevel())))
                .collect(Collectors.toList());
    }

    /**
     * Converts a singular proto payload to a format that can be used by the MaxCompute SDK.
     * @param protoPayload the proto payload to convert, containing field descriptor, the actual object and level
     * @return the converted object
     */
    Object convertSingularPayload(ProtoPayload protoPayload);

}
