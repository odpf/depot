package com.gotocompany.depot.maxcompute.client.ddl;

import com.aliyun.odps.Column;
import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.task.SQLTask;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.maxcompute.exception.MaxComputeTableOperationException;
import com.gotocompany.depot.maxcompute.schema.SchemaDifferenceUtils;
import com.gotocompany.depot.maxcompute.schema.validator.TableValidator;
import com.gotocompany.depot.metrics.Instrumentation;
import com.gotocompany.depot.metrics.MaxComputeMetrics;
import com.gotocompany.depot.utils.RetryUtils;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.util.Deque;
import java.util.LinkedList;
import java.util.Objects;

/**
 * DdlManager is responsible for creating and updating MaxCompute tables.
 */
@Slf4j
public class DdlManager {

    private final Odps odps;
    private final MaxComputeSinkConfig maxComputeSinkConfig;
    private final Instrumentation instrumentation;
    private final MaxComputeMetrics maxComputeMetrics;
    private final TableValidator tableValidator;

    public DdlManager(Odps odps, MaxComputeSinkConfig maxComputeSinkConfig, Instrumentation instrumentation, MaxComputeMetrics maxComputeMetrics) {
        this.odps = odps;
        this.maxComputeSinkConfig = maxComputeSinkConfig;
        this.instrumentation = instrumentation;
        this.maxComputeMetrics = maxComputeMetrics;
        this.tableValidator = new TableValidator(maxComputeSinkConfig);
    }

    /**
     * Creates or updates the MaxCompute table based on the table schema.
     * Creates the table if it does not exist, otherwise updates the table schema.
     * DDL operations are retried based on the configuration.
     *
     * @param tableSchema the table schema to create or update
     * @throws OdpsException if the table creation or update fails
     */
    public void createOrUpdateTable(TableSchema tableSchema) throws OdpsException {
        String projectName = maxComputeSinkConfig.getMaxComputeProjectId();
        String datasetName = maxComputeSinkConfig.getMaxComputeSchema();
        String tableName = maxComputeSinkConfig.getMaxComputeTableName();
        if (!this.odps.tables().exists(tableName)) {
            createTable(tableSchema, projectName, datasetName, tableName);
            return;
        }
        updateTable(tableSchema, projectName, datasetName, tableName);
    }

    private void createTable(TableSchema tableSchema, String projectName, String datasetName, String tableName) {
        log.info("Creating table: {} schema:{}", tableName, tableSchema);
        tableValidator.validate(tableName, maxComputeSinkConfig.getMaxComputeTableLifecycleDays(), tableSchema);
        RetryUtils.executeWithRetry(() -> {
                    Instant start = Instant.now();
                    this.odps.tables().create(projectName, datasetName, tableName, tableSchema, "",
                            true, maxComputeSinkConfig.getMaxComputeTableLifecycleDays(),
                            null, null);
                    instrumentation.logInfo("Successfully created maxCompute table " + tableName);
                    instrument(start, MaxComputeMetrics.MaxComputeAPIType.TABLE_CREATE);
                }, maxComputeSinkConfig.getMaxDdlRetryCount(), maxComputeSinkConfig.getDdlRetryBackoffMillis(),
                e -> e instanceof OdpsException);
    }

    private void updateTable(TableSchema tableSchema, String projectName, String datasetName, String tableName) {
        log.info("Updating table: {} schema:{}", tableName, tableSchema);
        Instant start = Instant.now();
        TableSchema oldSchema = this.odps.tables().get(projectName, datasetName, tableName)
                .getSchema();
        checkPartitionPrecondition(oldSchema);
        Deque<String> schemaDifferenceSql = new LinkedList<>(SchemaDifferenceUtils.getSchemaDifferenceSql(oldSchema, tableSchema, datasetName, tableName));
        RetryUtils.executeWithRetry(() -> {
                    while (!schemaDifferenceSql.isEmpty()) {
                        String sql = schemaDifferenceSql.peekFirst();
                        Instance instance = execute(sql);
                        if (!instance.isSuccessful()) {
                            instrumentation.logError("Failed to execute SQL: " + sql);
                            String errorMessage = instance.getRawTaskResults().get(0).getResult().getString();
                            throw new MaxComputeTableOperationException(String.format("Failed to update table schema with reason: %s", errorMessage));
                        }
                        schemaDifferenceSql.pollFirst();
                    }
                    instrumentation.logInfo("Successfully updated maxCompute table " + tableName);
                    instrument(start, MaxComputeMetrics.MaxComputeAPIType.TABLE_UPDATE);
                }, maxComputeSinkConfig.getMaxDdlRetryCount(), maxComputeSinkConfig.getDdlRetryBackoffMillis(),
                e -> e instanceof OdpsException);
    }

    private void checkPartitionPrecondition(TableSchema oldSchema) {
        if (maxComputeSinkConfig.isTablePartitioningEnabled() && oldSchema.getPartitionColumns().isEmpty()) {
            throw new MaxComputeTableOperationException("Updating non-partitioned table to partitioned table is not supported");
        }
        if (maxComputeSinkConfig.isTablePartitioningEnabled()) {
            String currentPartitionColumnKey = oldSchema.getPartitionColumns()
                    .stream()
                    .findFirst()
                    .map(Column::getName)
                    .orElse(null);
            if (!Objects.equals(maxComputeSinkConfig.getTablePartitionColumnName(), currentPartitionColumnKey)) {
                throw new MaxComputeTableOperationException("Changing partition column is not supported");
            }
        }
    }

    private Instance execute(String sql) throws OdpsException {
        log.info("Executing SQL: {}", sql);
        Instance instance = SQLTask.run(odps, sql);
        instance.waitForSuccess();
        return instance;
    }

    private void instrument(Instant startTime, MaxComputeMetrics.MaxComputeAPIType type) {
        instrumentation.incrementCounter(
                maxComputeMetrics.getMaxComputeOperationTotalMetric(),
                String.format(MaxComputeMetrics.MAXCOMPUTE_TABLE_TAG, maxComputeSinkConfig.getMaxComputeTableName()),
                String.format(MaxComputeMetrics.MAXCOMPUTE_PROJECT_TAG, maxComputeSinkConfig.getMaxComputeProjectId()),
                String.format(MaxComputeMetrics.MAXCOMPUTE_API_TAG, type)
        );
        instrumentation.captureDurationSince(
                maxComputeMetrics.getMaxComputeOperationLatencyMetric(),
                startTime,
                String.format(MaxComputeMetrics.MAXCOMPUTE_TABLE_TAG, maxComputeSinkConfig.getMaxComputeTableName()),
                String.format(MaxComputeMetrics.MAXCOMPUTE_PROJECT_TAG, maxComputeSinkConfig.getMaxComputeProjectId()),
                String.format(MaxComputeMetrics.MAXCOMPUTE_API_TAG, type)
        );
    }

}
