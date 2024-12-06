package com.gotocompany.depot.maxcompute.model;

import com.google.protobuf.Descriptors;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@Getter
public class ProtoPayload {
    private final Descriptors.FieldDescriptor fieldDescriptor;
    private final Object parsedObject;
    private final boolean isRootLevel;
}
