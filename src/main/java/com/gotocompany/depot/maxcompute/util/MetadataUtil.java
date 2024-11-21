package com.gotocompany.depot.maxcompute.util;

import com.aliyun.odps.type.StructTypeInfo;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.gotocompany.depot.common.TupleString;
import com.gotocompany.depot.config.MaxComputeSinkConfig;

import java.time.Instant;
import java.util.HashMap;
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
        METADATA_TYPE_MAP = new HashMap<>();
        METADATA_TYPE_MAP.put(INTEGER, TypeInfoFactory.INT);
        METADATA_TYPE_MAP.put(LONG, TypeInfoFactory.BIGINT);
        METADATA_TYPE_MAP.put(FLOAT, TypeInfoFactory.FLOAT);
        METADATA_TYPE_MAP.put(DOUBLE, TypeInfoFactory.DOUBLE);
        METADATA_TYPE_MAP.put(STRING, TypeInfoFactory.STRING);
        METADATA_TYPE_MAP.put(BOOLEAN, TypeInfoFactory.BOOLEAN);
        METADATA_TYPE_MAP.put(TIMESTAMP, TypeInfoFactory.TIMESTAMP_NTZ);

        METADATA_MAPPER_MAP = new HashMap<>();
        METADATA_MAPPER_MAP.put("integer", obj -> ((Number) obj).intValue());
        METADATA_MAPPER_MAP.put("long", obj -> ((Number) obj).longValue());
        METADATA_MAPPER_MAP.put("float", obj -> ((Number) obj).floatValue());
        METADATA_MAPPER_MAP.put("double", obj -> ((Number) obj).doubleValue());
        METADATA_MAPPER_MAP.put("string", Function.identity());
        METADATA_MAPPER_MAP.put("boolean", Function.identity());
    }

    public static TypeInfo getMetadataTypeInfo(String type) {
        return METADATA_TYPE_MAP.get(type.toLowerCase());
    }

    public static Object getValidMetadataValue(String type, Object value, MaxComputeSinkConfig maxComputeSinkConfig) {
        if (TIMESTAMP.equalsIgnoreCase(type)) {
            return Instant.ofEpochMilli((Long) value)
                    .atZone(maxComputeSinkConfig.getZoneId())
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
