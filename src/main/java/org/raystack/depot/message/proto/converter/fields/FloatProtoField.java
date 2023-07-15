package org.raystack.depot.message.proto.converter.fields;

import com.google.protobuf.Descriptors;

import java.util.Collection;
import java.util.stream.Collectors;

public class FloatProtoField implements ProtoField {
    private final Object fieldValue;
    private final Descriptors.FieldDescriptor descriptor;

    public FloatProtoField(Descriptors.FieldDescriptor descriptor, Object fieldValue) {
        this.descriptor = descriptor;
        this.fieldValue = fieldValue;
    }

    @Override
    public Object getValue() {
        if (fieldValue instanceof Collection<?>) {
            return ((Collection<?>) fieldValue).stream().map(this::getValue).collect(Collectors.toList());
        }
        return getValue(fieldValue);
    }

    public Double getValue(Object field) {
        double val = Double.parseDouble(field.toString());
        boolean valid = !Double.isInfinite(val) && !Double.isNaN(val);
        if (!valid) {
            throw new IllegalArgumentException("Float/double value is not valid");
        }
        return val;
    }

    @Override
    public boolean matches() {
        return descriptor.getType() == Descriptors.FieldDescriptor.Type.FLOAT
                || descriptor.getType() == Descriptors.FieldDescriptor.Type.DOUBLE;
    }
}
