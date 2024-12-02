package com.gotocompany.depot.maxcompute.converter.payload;

import com.google.protobuf.Descriptors;
import com.gotocompany.depot.maxcompute.model.ProtoPayload;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Interface for converting payloads based on field descriptors.
 */
public interface ProtobufPayloadConverter {

    /**
     * Converts a proto payload to a format that can be used by the MaxCompute SDK.
     * @param protoPayload the proto payload to convert, containing field descriptor, the actual object and level
     * @return
     */
    default Object convert(ProtoPayload protoPayload) {
        if (!protoPayload.getFieldDescriptor().isRepeated()) {
            return convertSingular(protoPayload);
        }
        return ((List<?>) protoPayload.getObject()).stream()
                .map(o -> convertSingular(new ProtoPayload(protoPayload.getFieldDescriptor(), o, protoPayload.isRootLevel())))
                .collect(Collectors.toList());
    }

    /**
     * Converts a singular proto payload to a format that can be used by the MaxCompute SDK.
     * @param protoPayload the proto payload to convert, containing field descriptor, the actual object and level
     * @return
     */
    Object convertSingular(ProtoPayload protoPayload);

    /**
     * Checks if the converter can convert the given field descriptor.
     *
     * @param fieldDescriptor the field descriptor
     * @return true if the converter can convert the field descriptor, false otherwise
     */
    boolean canConvert(Descriptors.FieldDescriptor fieldDescriptor);
}
