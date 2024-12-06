package com.gotocompany.depot.maxcompute.converter.type;

import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.protobuf.Descriptors;

/**
 * Interface for converting Protobuf field descriptors to MaxCompute TypeInfo.
 */
public interface ProtobufTypeInfoConverter {

    /**
     * Converts a Protobuf field descriptor to a MaxCompute TypeInfo.
     * This method wraps the singular type conversion with array type handling if the field is repeated.
     *
     * @param fieldDescriptor the Protobuf field descriptor to convert
     * @return the corresponding MaxCompute TypeInfo
     */
    default TypeInfo convert(Descriptors.FieldDescriptor fieldDescriptor) {
        return wrap(fieldDescriptor, convertSingular(fieldDescriptor));
    }

    /**
     * Converts a singular Protobuf field descriptor to a MaxCompute TypeInfo.
     * This method should be implemented by subclasses to handle specific field types.
     *
     * @param fieldDescriptor the Protobuf field descriptor to convert
     * @return the corresponding MaxCompute TypeInfo for the singular field
     */
    TypeInfo convertSingular(Descriptors.FieldDescriptor fieldDescriptor);

    /**
     * Checks if the converter can handle the given Protobuf field descriptor.
     *
     * @param fieldDescriptor the Protobuf field descriptor to check
     * @return true if the converter can handle the field descriptor, false otherwise
     */
    boolean canConvert(Descriptors.FieldDescriptor fieldDescriptor);

    /**
     * Wraps the singular TypeInfo with array type handling if the field is repeated.
     *
     * @param fieldDescriptor the Protobuf field descriptor
     * @param typeInfo the singular TypeInfo to wrap
     * @return the wrapped TypeInfo, handling repeated fields as arrays
     */
    default TypeInfo wrap(Descriptors.FieldDescriptor fieldDescriptor, TypeInfo typeInfo) {
        return fieldDescriptor.isRepeated() ? TypeInfoFactory.getArrayTypeInfo(typeInfo) : typeInfo;
    }
}