package com.gotocompany.depot.maxcompute.util;

import com.aliyun.odps.type.StructTypeInfo;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.gotocompany.depot.common.TupleString;
import com.gotocompany.depot.config.MaxComputeSinkConfig;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class MetadataUtil {

    private static final Map<String, TypeInfo> METADATA_TYPE_MAP;
    private static final Map<String, Function<Object, Object>> METADATA_MAPPER_MAP;

    static {
        METADATA_TYPE_MAP = new HashMap<>();
        METADATA_TYPE_MAP.put("integer", TypeInfoFactory.INT);
        METADATA_TYPE_MAP.put("long", TypeInfoFactory.BIGINT);
        METADATA_TYPE_MAP.put("float", TypeInfoFactory.FLOAT);
        METADATA_TYPE_MAP.put("double", TypeInfoFactory.DOUBLE);
        METADATA_TYPE_MAP.put("string", TypeInfoFactory.STRING);
        METADATA_TYPE_MAP.put("boolean", TypeInfoFactory.BOOLEAN);
        METADATA_TYPE_MAP.put("timestamp", TypeInfoFactory.TIMESTAMP_NTZ);

        METADATA_MAPPER_MAP = new HashMap<>();
        METADATA_MAPPER_MAP.put("integer", obj -> ((Number) obj).intValue());
        METADATA_MAPPER_MAP.put("long", obj -> ((Number) obj).longValue());
        METADATA_MAPPER_MAP.put("float", obj -> ((Number) obj).floatValue());
        METADATA_MAPPER_MAP.put("double", obj -> ((Number) obj).doubleValue());
        METADATA_MAPPER_MAP.put("string", Function.identity());
        METADATA_MAPPER_MAP.put("boolean", Function.identity());
        METADATA_MAPPER_MAP.put("timestamp", obj -> LocalDateTime.ofEpochSecond((Long) obj,
                0, ZoneOffset.UTC));
    }

    public static TypeInfo getMetadataTypeInfo(String type) {
        return METADATA_TYPE_MAP.get(type.toLowerCase());
    }

    public static Object getValidMetadataValue(String type, Object value) {
        return METADATA_MAPPER_MAP.get(type.toLowerCase()).apply(value);
    }

    public static StructTypeInfo getMetadataTypeInfo(MaxComputeSinkConfig maxComputeSinkConfig) {
        return TypeInfoFactory.getStructTypeInfo(maxComputeSinkConfig.getMetadataColumnsTypes()
                        .stream()
                        .map(TupleString::getFirst)
                        .collect(Collectors.toList()),
                maxComputeSinkConfig.getMetadataColumnsTypes()
                        .stream()
                        .map(tuple -> METADATA_TYPE_MAP.get(tuple.getSecond().toLowerCase()))
                        .collect(Collectors.toList()));
    }

}
