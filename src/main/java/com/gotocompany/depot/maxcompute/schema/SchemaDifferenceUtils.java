package com.gotocompany.depot.maxcompute.schema;

import com.aliyun.odps.TableSchema;
import com.aliyun.odps.type.ArrayTypeInfo;
import com.aliyun.odps.type.StructTypeInfo;
import com.aliyun.odps.type.TypeInfo;
import com.gotocompany.depot.maxcompute.model.MaxComputeColumnDetail;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static com.gotocompany.depot.maxcompute.util.TypeInfoUtils.isPrimitiveArrayType;
import static com.gotocompany.depot.maxcompute.util.TypeInfoUtils.isPrimitiveType;
import static com.gotocompany.depot.maxcompute.util.TypeInfoUtils.isStructArrayType;
import static com.gotocompany.depot.maxcompute.util.TypeInfoUtils.isStructType;

public class SchemaDifferenceUtils {

    private static final String ALTER_TABLE_QUERY_TEMPLATE = "ALTER TABLE %s.%s ADD COLUMN IF NOT EXISTS %s;";

    public static List<String> getSchemaDifferenceSql(TableSchema oldSchema, TableSchema newSchema, String schemaName, String tableName) {
        List<MaxComputeColumnDetail> maxComputeColumnDetailDifference = getMaxComputeColumnDetailDifference(oldSchema, newSchema, tableName);

        return maxComputeColumnDetailDifference.stream()
                .map(MaxComputeColumnDetail -> String.format(ALTER_TABLE_QUERY_TEMPLATE, schemaName, tableName, MaxComputeColumnDetail.getDdlDeclaration()))
                .collect(Collectors.toList());
    }

    private static List<MaxComputeColumnDetail> getMaxComputeColumnDetailDifference(TableSchema oldSchema, TableSchema newSchema, String tableName) {
        Map<String, MaxComputeColumnDetail> oldMaxComputeColumnDetail = buildMaxComputeColumnDetailMap(oldSchema);
        Map<String, MaxComputeColumnDetail> newMaxComputeColumnDetail = buildMaxComputeColumnDetailMap(newSchema);
        Iterator<Map.Entry<String, MaxComputeColumnDetail>> newMaxComputeColumnDetailIterator = newMaxComputeColumnDetail.entrySet().iterator();
        List<MaxComputeColumnDetail> changedMetadata = new ArrayList<>();

        while (newMaxComputeColumnDetailIterator.hasNext()) {
            Map.Entry<String, MaxComputeColumnDetail> entry = newMaxComputeColumnDetailIterator.next();
            String columnName = entry.getKey();
            MaxComputeColumnDetail oldMetadata = oldMaxComputeColumnDetail.get(columnName);
            if (Objects.isNull(oldMetadata)) { //handle new column / struct field
                changedMetadata.add(entry.getValue());
                if (isStructType(entry.getValue().getTypeInfo()) || isStructArrayType(entry.getValue().getTypeInfo())) {
                    skipStructFields(entry, newMaxComputeColumnDetailIterator);
                }
            }
        }
        return changedMetadata;
    }

    private static void skipStructFields(Map.Entry<String, MaxComputeColumnDetail> entry, Iterator<Map.Entry<String, MaxComputeColumnDetail>> newMaxComputeColumnDetailIterator) {
        StructTypeInfo structTypeInfo = isStructType(entry.getValue().getTypeInfo()) ? (StructTypeInfo) entry.getValue().getTypeInfo()
                : ((StructTypeInfo) ((ArrayTypeInfo) entry.getValue().getTypeInfo()).getElementTypeInfo());
        for (int i = 0; i < structTypeInfo.getFieldCount(); i++) {
            newMaxComputeColumnDetailIterator.next();
        }
    }

    private static Map<String, MaxComputeColumnDetail> buildMaxComputeColumnDetailMap(TableSchema schema) {
        Map<String, MaxComputeColumnDetail> maxComputeColumnDetailMap = new TreeMap<>();
        schema.getColumns().forEach(column -> fieldMetadataHelper(column.getTypeInfo(), "", column.getName(), maxComputeColumnDetailMap, false));
        return maxComputeColumnDetailMap;
    }

    private static void fieldMetadataHelper(TypeInfo typeInfo, String prefix, String name, Map<String, MaxComputeColumnDetail> result, boolean isArrayElement) {
        if (isPrimitiveType(typeInfo) || isPrimitiveArrayType(typeInfo)) {
            result.put(getPathName(prefix, name, isArrayElement), new MaxComputeColumnDetail(prefix, name, typeInfo, isArrayElement));
        }
        if (isStructType(typeInfo) || isStructArrayType(typeInfo)) {
            StructTypeInfo structTypeInfo = isStructType(typeInfo) ? (StructTypeInfo) typeInfo : ((StructTypeInfo) ((ArrayTypeInfo) typeInfo).getElementTypeInfo());
            result.put(getPathName(prefix, name, isArrayElement), new MaxComputeColumnDetail(prefix, name, typeInfo, isArrayElement));

            for (int i = 0; i < structTypeInfo.getFieldCount(); i++) {
                TypeInfo fieldType = structTypeInfo.getFieldTypeInfos().get(i);
                String fieldName = structTypeInfo.getFieldNames().get(i);
                fieldMetadataHelper(fieldType, getPathName(prefix, name, isArrayElement), fieldName, result, isStructArrayType(typeInfo));
            }
        }
    }

    private static String getPathName(String prefix, String name, boolean isArrayElement) {
        return StringUtils.isBlank(prefix) ? name : String.format("%s%s.%s", prefix, isArrayElement ? ".element" : "", name);
    }
}
