package com.gotocompany.depot.maxcompute.converter.payload;

import com.google.protobuf.Descriptors;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Interface for converting payloads based on field descriptors.
 */
public interface ProtobufPayloadConverter {

    /**
     * Converts the given object based on the field descriptor.
     * If the field is repeated, it converts each element in the list.
     *
     * @param fieldDescriptor the field descriptor
     * @param object the object to convert
     * @return the converted object
     */
    default Object convert(Descriptors.FieldDescriptor fieldDescriptor, Object object) {
        if (!fieldDescriptor.isRepeated()) {
            return convertSingular(fieldDescriptor, object);
        }
        return ((List<?>) object).stream()
                .map(o -> convertSingular(fieldDescriptor, o))
                .collect(Collectors.toList());
    }

    /**
     * Converts a singular object based on the field descriptor.
     *
     * @param fieldDescriptor the field descriptor
     * @param object the object to convert
     * @return the converted object
     */
    Object convertSingular(Descriptors.FieldDescriptor fieldDescriptor, Object object);

    /**
     * Checks if the converter can convert the given field descriptor.
     *
     * @param fieldDescriptor the field descriptor
     * @return true if the converter can convert the field descriptor, false otherwise
     */
    boolean canConvert(Descriptors.FieldDescriptor fieldDescriptor);
}
