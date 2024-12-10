package com.gotocompany.depot.maxcompute.converter;

import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Duration;
import com.gotocompany.depot.TestMaxComputeTypeInfo;
import com.gotocompany.depot.maxcompute.model.ProtoPayload;
import org.assertj.core.api.Assertions;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DurationProtobufMaxComputeConverterTest {

    private static final int DURATION_INDEX = 5;
    private final Descriptors.Descriptor descriptor = TestMaxComputeTypeInfo.TestRoot.getDescriptor();
    private final DurationProtobufMaxComputeConverter durationProtobufMaxComputeConverter = new DurationProtobufMaxComputeConverter();
    private final Descriptors.Descriptor repeatedDescriptor = TestMaxComputeTypeInfo.TestRootRepeated.getDescriptor();

    @Test
    public void shouldConvertToStruct() {
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.getFields().get(DURATION_INDEX);

        TypeInfo typeInfo = durationProtobufMaxComputeConverter.convertTypeInfo(fieldDescriptor);

        assertEquals("STRUCT<`seconds`:BIGINT,`nanos`:INT>", typeInfo.getTypeName());
    }

    @Test
    public void shouldReturnTrueForDuration() {
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.getFields().get(DURATION_INDEX);

        boolean canConvert = durationProtobufMaxComputeConverter.canConvert(fieldDescriptor);

        assertTrue(canConvert);
    }

    @Test
    public void shouldReturnFalseForNonDuration() {
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.getFields().get(0);

        boolean canConvert = durationProtobufMaxComputeConverter.canConvert(fieldDescriptor);

        assertFalse(canConvert);
    }

    @Test
    public void shouldConvertDurationPayloadToStruct() {
        Duration duration = Duration.newBuilder()
                .setSeconds(1)
                .setNanos(1)
                .build();
        TestMaxComputeTypeInfo.TestRoot message = TestMaxComputeTypeInfo.TestRoot.newBuilder()
                .setDurationField(duration)
                .build();
        List<String> expectedFieldNames = Arrays.asList("seconds", "nanos");
        List<TypeInfo> expectedTypeInfos = Arrays.asList(TypeInfoFactory.BIGINT, TypeInfoFactory.INT);
        List<Object> values = Arrays.asList(1L, 1);
        Object result = durationProtobufMaxComputeConverter.convertPayload(new ProtoPayload(descriptor.getFields().get(5), message.getField(descriptor.getFields().get(5)), true));

        assertThat(result)
                .isInstanceOf(com.aliyun.odps.data.SimpleStruct.class)
                .extracting("typeInfo", "values")
                .containsExactly(TypeInfoFactory.getStructTypeInfo(expectedFieldNames, expectedTypeInfos), values);
    }

    @Test
    public void shouldConvertRepeatedDurationPayloadToStructList() {
        Duration duration1 = Duration.newBuilder()
                .setSeconds(1)
                .setNanos(1)
                .build();
        Duration duration2 = Duration.newBuilder()
                .setSeconds(2)
                .setNanos(2)
                .build();
        TestMaxComputeTypeInfo.TestRootRepeated message = TestMaxComputeTypeInfo.TestRootRepeated.newBuilder()
                .addAllDurationFields(Arrays.asList(duration1, duration2))
                .build();
        List<String> expectedFieldNames = Arrays.asList("seconds", "nanos");
        List<TypeInfo> expectedTypeInfos = Arrays.asList(TypeInfoFactory.BIGINT, TypeInfoFactory.INT);
        List<Object> values1 = Arrays.asList(1L, 1);
        List<Object> values2 = Arrays.asList(2L, 2);

        Object result = durationProtobufMaxComputeConverter.convertPayload(new ProtoPayload(repeatedDescriptor.getFields().get(5), message.getField(repeatedDescriptor.getFields().get(5)), true));

        assertThat(result)
                .isInstanceOf(List.class);
        assertThat((List<?>) result)
                .hasSize(2)
                .allMatch(element -> element instanceof com.aliyun.odps.data.SimpleStruct)
                .extracting("typeInfo", "values")
                .containsExactly(
                        Assertions.tuple(TypeInfoFactory.getStructTypeInfo(expectedFieldNames, expectedTypeInfos), values1),
                        Assertions.tuple(TypeInfoFactory.getStructTypeInfo(expectedFieldNames, expectedTypeInfos), values2)
                );
    }

}
