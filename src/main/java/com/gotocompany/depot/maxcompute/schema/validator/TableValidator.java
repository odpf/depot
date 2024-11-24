package com.gotocompany.depot.maxcompute.schema.validator;

import com.aliyun.odps.TableSchema;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;

public class TableValidator {

    private static final Pattern VALID_TABLE_NAME_REGEX = Pattern.compile("^[A-Za-z][A-Za-z0-9_]{0,127}$");
    private static final int MAX_COLUMNS_PER_TABLE = 1200;
    private static final int MAX_PARTITION_KEYS_PER_TABLE = 6;

    public static void validate(String tableName, Long lifecycleDays, TableSchema tableSchema) {
        List<String> errorHolder = new ArrayList<>();
        validateTableName(tableName, errorHolder);
        validateLifecycleDays(lifecycleDays, errorHolder);
        validateTableSchema(tableSchema, errorHolder);
        if (!errorHolder.isEmpty()) {
            throw new IllegalArgumentException(String.join(", ", errorHolder));
        }
    }

    private static void validateTableName(String tableName, List<String> errorHolder) {
        if (!VALID_TABLE_NAME_REGEX.matcher(tableName).matches()) {
            errorHolder.add("Table name should match the pattern: " + VALID_TABLE_NAME_REGEX.pattern());
        }
    }

    private static void validateLifecycleDays(Long lifecycleDays, List<String> errorHolder) {
        if (Objects.nonNull(lifecycleDays) && lifecycleDays < 0) {
            errorHolder.add("Lifecycle days should be a positive integer");
        }
    }

    private static void validateTableSchema(TableSchema tableSchema, List<String> errorHolder) {
        if (tableSchema.getAllColumns().size() >= MAX_COLUMNS_PER_TABLE) {
            errorHolder.add("Table schema should have less than " + MAX_COLUMNS_PER_TABLE + " columns");
        }
        if (tableSchema.getPartitionColumns().size() > MAX_PARTITION_KEYS_PER_TABLE) {
            errorHolder.add("Table schema should have less than " + MAX_PARTITION_KEYS_PER_TABLE + " partition keys");
        }
    }

}
