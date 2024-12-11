package com.gotocompany.depot.maxcompute.util;

import com.aliyun.odps.type.StructTypeInfo;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.common.collect.ImmutableMap;
import com.gotocompany.depot.common.TupleString;

import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class MetadataUtil {

    private static final Map<String, TypeInfo> METADATA_TYPE_MAP;
    private static final Map<String, Function<Object, Object>> METADATA_MAPPER_MAP;

    private static final String TIMESTAMP = "timestamp";
    private static final String INTEGER = "integer";
    private static final String LONG = "long";
    private static final String FLOAT = "float";
    private static final String DOUBLE = "double";
    private static final String STRING = "string";
    private static final String BOOLEAN = "boolean";

    static {
        METADATA_TYPE_MAP = ImmutableMap.<String, TypeInfo>builder()
                .put(INTEGER, TypeInfoFactory.INT)
                .put(LONG, TypeInfoFactory.BIGINT)
                .put(FLOAT, TypeInfoFactory.FLOAT)
                .put(DOUBLE, TypeInfoFactory.DOUBLE)
                .put(STRING, TypeInfoFactory.STRING)
                .put(BOOLEAN, TypeInfoFactory.BOOLEAN)
                .put(TIMESTAMP, TypeInfoFactory.TIMESTAMP_NTZ)
                .build();

        METADATA_MAPPER_MAP = ImmutableMap.<String, Function<Object, Object>>builder()
                .put(INTEGER, obj -> ((Number) obj).intValue())
                .put(LONG, obj -> ((Number) obj).longValue())
                .put(FLOAT, obj -> ((Number) obj).floatValue())
                .put(DOUBLE, obj -> ((Number) obj).doubleValue())
                .put(STRING, Function.identity())
                .put(BOOLEAN, Function.identity())
                .build();
    }

    public static TypeInfo getMetadataTypeInfo(String type) {
        return METADATA_TYPE_MAP.get(type.toLowerCase());
    }

    public static Object getValidMetadataValue(String type, Object value, ZoneId zoneId) {
        if (TIMESTAMP.equalsIgnoreCase(type) && value instanceof Long) {
            return Instant.ofEpochMilli((Long) value)
                    .atZone(zoneId)
                    .toLocalDateTime();
        }
        return METADATA_MAPPER_MAP.get(type.toLowerCase()).apply(value);
    }

    public static StructTypeInfo getMetadataTypeInfo(List<TupleString> metadataColumnsTypes) {
        return TypeInfoFactory.getStructTypeInfo(metadataColumnsTypes
                        .stream()
                        .map(TupleString::getFirst)
                        .collect(Collectors.toList()),
                metadataColumnsTypes
                        .stream()
                        .map(tuple -> METADATA_TYPE_MAP.get(tuple.getSecond().toLowerCase()))
                        .collect(Collectors.toList()));
    }

}
