package com.gotocompany.depot.maxcompute.converter;

import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.protobuf.Descriptors;
import com.gotocompany.depot.TestMaxComputeProtobufConverterCacheOuterClass;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Collections;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class MaxComputeProtobufConverterCacheTest {

    private final Descriptors.Descriptor descriptor = TestMaxComputeProtobufConverterCacheOuterClass.TestMaxComputeProtobufConverterCache.getDescriptor();
    private MaxComputeProtobufConverterCache maxComputeProtobufConverterCache;

    @Before
    public void setup() {
        MaxComputeSinkConfig maxComputeSinkConfig = mock(MaxComputeSinkConfig.class);
        when(maxComputeSinkConfig.getMaxPastYearEventTimeDifference()).thenReturn(5);
        when(maxComputeSinkConfig.getMaxFutureYearEventTimeDifference()).thenReturn(5);
        when(maxComputeSinkConfig.getZoneId()).thenReturn(ZoneId.of("UTC"));
        when(maxComputeSinkConfig.getValidMinTimestamp()).thenReturn(LocalDateTime.MIN);
        when(maxComputeSinkConfig.getValidMaxTimestamp()).thenReturn(LocalDateTime.MAX);
        when(maxComputeSinkConfig.isTablePartitioningEnabled()).thenReturn(true);
        when(maxComputeSinkConfig.getTablePartitionKey()).thenReturn("partition_key");
        maxComputeProtobufConverterCache = new MaxComputeProtobufConverterCache(maxComputeSinkConfig);
    }

    @Test
    public void shouldCreateTypeInfoIfNotExists() throws NoSuchFieldException, IllegalAccessException {
        Field cacheField = maxComputeProtobufConverterCache.getClass()
                .getDeclaredField("typeInfoCache");
        cacheField.setAccessible(true);
        Map<String, TypeInfo> typeInfoCache = ((Map<String, TypeInfo>) cacheField.get(maxComputeProtobufConverterCache));
        assertEquals(0, typeInfoCache.size());

        TypeInfo typeInfo = maxComputeProtobufConverterCache.getOrCreateTypeInfo(descriptor.findFieldByName("string_field"));

        assertEquals(1, typeInfoCache.size());
        assertEquals(typeInfo, typeInfoCache.get(descriptor.findFieldByName("string_field").getFullName()));
    }

    @Test
    public void shouldGetCachedTypeInfo() throws NoSuchFieldException, IllegalAccessException {
        Field cacheField = maxComputeProtobufConverterCache.getClass().getDeclaredField("typeInfoCache");
        cacheField.setAccessible(true);
        maxComputeProtobufConverterCache.getOrCreateTypeInfo(descriptor.findFieldByName("string_field"));
        Map<String, TypeInfo> typeInfoCache = Mockito.spy((Map<String, TypeInfo>) cacheField.get(maxComputeProtobufConverterCache));
        assertEquals(1, typeInfoCache.size());

        TypeInfo typeInfo = maxComputeProtobufConverterCache.getOrCreateTypeInfo(descriptor.findFieldByName("string_field"));

        assertEquals(1, typeInfoCache.size());
        verify(typeInfoCache, Mockito.times(0)).put(descriptor.findFieldByName("string_field").getFullName(), typeInfo);
        assertEquals(typeInfo, typeInfoCache.get(descriptor.findFieldByName("string_field").getFullName()));
    }

    @Test
    public void shouldCreateTypeInfoIfNotExistsFromSupplierLogic() throws NoSuchFieldException, IllegalAccessException {
        TypeInfo expectedTypeInfo = TypeInfoFactory.getStructTypeInfo(Collections.singletonList("string_field"), Collections.singletonList(TypeInfoFactory.STRING));
        Field cacheField = maxComputeProtobufConverterCache.getClass()
                .getDeclaredField("typeInfoCache");
        cacheField.setAccessible(true);
        Map<String, TypeInfo> typeInfoCache = ((Map<String, TypeInfo>) cacheField.get(maxComputeProtobufConverterCache));
        assertEquals(0, typeInfoCache.size());

        TypeInfo typeInfo = maxComputeProtobufConverterCache.getOrCreateTypeInfo(descriptor.findFieldByName("inner_message_field"),
                () -> expectedTypeInfo);

        assertEquals(1, typeInfoCache.size());
        assertEquals(typeInfo, typeInfoCache.get(descriptor.findFieldByName("inner_message_field").getFullName()));
    }

    @Test
    public void shouldGetTypeInfoFromCacheWithSupplierLogic() throws NoSuchFieldException, IllegalAccessException {
        TypeInfo expectedTypeInfo = TypeInfoFactory.getStructTypeInfo(Collections.singletonList("string_field"), Collections.singletonList(TypeInfoFactory.STRING));
        Field cacheField = maxComputeProtobufConverterCache.getClass()
                .getDeclaredField("typeInfoCache");
        cacheField.setAccessible(true);
        Map<String, TypeInfo> typeInfoCache = ((Map<String, TypeInfo>) cacheField.get(maxComputeProtobufConverterCache));
        maxComputeProtobufConverterCache.getOrCreateTypeInfo(descriptor.findFieldByName("inner_message_field"),
                () -> expectedTypeInfo);
        assertEquals(1, typeInfoCache.size());

        TypeInfo typeInfo = maxComputeProtobufConverterCache.getOrCreateTypeInfo(descriptor.findFieldByName("inner_message_field"),
                () -> expectedTypeInfo);

        assertEquals(1, typeInfoCache.size());
        assertEquals(typeInfo, typeInfoCache.get(descriptor.findFieldByName("inner_message_field").getFullName()));
    }

    @Test
    public void shouldGetConverterForPrimitive() {
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("string_field");

        ProtobufMaxComputeConverter converter = maxComputeProtobufConverterCache.getConverter(fieldDescriptor);

        assertEquals(PrimitiveProtobufMaxComputeConverter.class, converter.getClass());
    }

    @Test
    public void shouldGetConverterForTimestamp() {
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("timestamp_field");

        ProtobufMaxComputeConverter converter = maxComputeProtobufConverterCache.getConverter(fieldDescriptor);

        assertEquals(TimestampProtobufMaxComputeConverter.class, converter.getClass());
    }

    @Test
    public void shouldGetConverterForDuration() {
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("duration_field");

        ProtobufMaxComputeConverter converter = maxComputeProtobufConverterCache.getConverter(fieldDescriptor);

        assertEquals(DurationProtobufMaxComputeConverter.class, converter.getClass());
    }

    @Test
    public void shouldGetConverterForStruct() {
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("struct_field");

        ProtobufMaxComputeConverter converter = maxComputeProtobufConverterCache.getConverter(fieldDescriptor);

        assertEquals(StructProtobufMaxComputeConverter.class, converter.getClass());
    }

    @Test
    public void shouldGetConverterForMessage() {
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("inner_message_field");

        ProtobufMaxComputeConverter converter = maxComputeProtobufConverterCache.getConverter(fieldDescriptor);

        assertEquals(MessageProtobufMaxComputeConverter.class, converter.getClass());
    }

    @Test
    public void shouldClearTypeInfoCache() throws NoSuchFieldException, IllegalAccessException {
        Field cacheField = maxComputeProtobufConverterCache.getClass()
                .getDeclaredField("typeInfoCache");
        cacheField.setAccessible(true);
        Map<String, TypeInfo> typeInfoCache = ((Map<String, TypeInfo>) cacheField.get(maxComputeProtobufConverterCache));
        maxComputeProtobufConverterCache.getOrCreateTypeInfo(descriptor.findFieldByName("string_field"));
        assertEquals(1, typeInfoCache.size());

        maxComputeProtobufConverterCache.clearCache();

        assertEquals(0, typeInfoCache.size());
    }

}
