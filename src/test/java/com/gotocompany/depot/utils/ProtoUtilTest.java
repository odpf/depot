package com.gotocompany.depot.utils;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.UnknownFieldSet;
import com.gotocompany.depot.TestBookingLogMessage;
import com.gotocompany.depot.TestLocation;
import com.gotocompany.depot.message.ProtoUnknownFieldValidationType;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ProtoUtilTest {
    @Test
    public void shouldReturnTrueWhenUnknownFieldsExistOnRootLevelFields() {
        Descriptors.Descriptor bookingLogMessage = TestBookingLogMessage.getDescriptor();
        Descriptors.Descriptor location = TestLocation.getDescriptor();

        Descriptors.FieldDescriptor fieldDescriptor = bookingLogMessage.findFieldByName("driver_pickup_location");
        DynamicMessage dynamicMessage = DynamicMessage.newBuilder(bookingLogMessage)
                .setField(fieldDescriptor, DynamicMessage.newBuilder(location)
                        .build())
                .setUnknownFields(UnknownFieldSet.newBuilder()
                        .addField(1, UnknownFieldSet.Field.getDefaultInstance())
                        .addField(2, UnknownFieldSet.Field.getDefaultInstance())
                        .build())
                .build();

        boolean unknownFieldExist = ProtoUtils.hasUnknownField(dynamicMessage, ProtoUnknownFieldValidationType.MESSAGE);
        assertTrue(unknownFieldExist);
    }

    @Test
    public void shouldReturnTrueWhenUnknownFieldsExistOnNestedChildFields() {
        Descriptors.Descriptor bookingLogMessage = TestBookingLogMessage.getDescriptor();
        Descriptors.Descriptor location = TestLocation.getDescriptor();
        Descriptors.FieldDescriptor fieldDescriptor = bookingLogMessage.findFieldByName("driver_pickup_location");

        DynamicMessage dynamicMessage = DynamicMessage.newBuilder(bookingLogMessage)
                .setField(fieldDescriptor, DynamicMessage.newBuilder(location)
                        .setUnknownFields(UnknownFieldSet.newBuilder()
                                .addField(1, UnknownFieldSet.Field.getDefaultInstance())
                                .addField(2, UnknownFieldSet.Field.getDefaultInstance())
                                .build())
                        .build())
                .build();

        boolean unknownFieldExist = ProtoUtils.hasUnknownField(dynamicMessage, ProtoUnknownFieldValidationType.MESSAGE);
        assertTrue(unknownFieldExist);
    }

    @Test
    public void shouldReturnFalseWhenNoUnknownFieldsExist() {
        Descriptors.Descriptor bookingLogMessage = TestBookingLogMessage.getDescriptor();
        Descriptors.Descriptor location = TestLocation.getDescriptor();

        Descriptors.FieldDescriptor fieldDescriptor = bookingLogMessage.findFieldByName("driver_pickup_location");
        DynamicMessage dynamicMessage = DynamicMessage.newBuilder(bookingLogMessage)
                .setField(fieldDescriptor, DynamicMessage.newBuilder(location).build())
                .build();

        boolean unknownFieldExist = ProtoUtils.hasUnknownField(dynamicMessage, ProtoUnknownFieldValidationType.MESSAGE);
        assertFalse(unknownFieldExist);
    }

    @Test
    public void shouldReturnFalseWhenRootIsNull() {
        boolean unknownFieldExist = ProtoUtils.hasUnknownField(null, ProtoUnknownFieldValidationType.MESSAGE);
        assertFalse(unknownFieldExist);
    }
}

