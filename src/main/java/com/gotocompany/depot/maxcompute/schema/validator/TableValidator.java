package com.gotocompany.depot.maxcompute.schema.validator;

import com.aliyun.odps.TableSchema;
import com.gotocompany.depot.config.MaxComputeSinkConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;

public class TableValidator {

    private final MaxComputeSinkConfig maxComputeSinkConfig;
    private final Pattern validTableNamePattern;

    public TableValidator(MaxComputeSinkConfig maxComputeSinkConfig) {
        this.maxComputeSinkConfig = maxComputeSinkConfig;
        this.validTableNamePattern = Pattern.compile(maxComputeSinkConfig.getTableValidatorNameRegex());

    }

    public void validate(String tableName, Long lifecycleDays, TableSchema tableSchema) {
        List<String> errorHolder = new ArrayList<>();
        validateTableName(tableName, errorHolder);
        validateLifecycleDays(lifecycleDays, errorHolder);
        validateTableSchema(tableSchema, errorHolder);
        if (!errorHolder.isEmpty()) {
            throw new IllegalArgumentException(String.join(", ", errorHolder));
        }
    }

    private void validateTableName(String tableName, List<String> errorHolder) {
        if (!validTableNamePattern.matcher(tableName).matches()) {
            errorHolder.add("Table name should match the pattern: " + validTableNamePattern.pattern());
        }
    }

    private void validateLifecycleDays(Long lifecycleDays, List<String> errorHolder) {
        if (Objects.nonNull(lifecycleDays) && lifecycleDays < 0) {
            errorHolder.add("Lifecycle days should be a positive integer");
        }
    }

    private void validateTableSchema(TableSchema tableSchema, List<String> errorHolder) {
        if (tableSchema.getAllColumns().size() > maxComputeSinkConfig.getTableValidatorMaxColumnsPerTable()) {
            errorHolder.add("Table schema should have less or equal than " + maxComputeSinkConfig.getTableValidatorMaxColumnsPerTable() + " columns");
        }
        if (tableSchema.getPartitionColumns().size() > maxComputeSinkConfig.getTableValidatorMaxPartitionKeysPerTable()) {
            errorHolder.add("Table schema should have less or equal than " + maxComputeSinkConfig.getTableValidatorMaxPartitionKeysPerTable() + " partition keys");
        }
    }

}
