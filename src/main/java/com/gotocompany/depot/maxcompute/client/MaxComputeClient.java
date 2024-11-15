package com.gotocompany.depot.maxcompute.client;

import com.aliyun.odps.Column;
import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.task.SQLTask;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.maxcompute.client.insert.InsertManager;
import com.gotocompany.depot.maxcompute.client.insert.InsertManagerFactory;
import com.gotocompany.depot.maxcompute.exception.MaxComputeTableOperationException;
import com.gotocompany.depot.maxcompute.model.RecordWrapper;
import com.gotocompany.depot.maxcompute.schema.SchemaDifferenceUtils;
import com.gotocompany.depot.metrics.Instrumentation;
import com.gotocompany.depot.metrics.MaxComputeMetrics;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@AllArgsConstructor
@NoArgsConstructor
@Slf4j
public class MaxComputeClient {

    private Odps odps;
    private MaxComputeSinkConfig maxComputeSinkConfig;
    private TableTunnel tableTunnel;
    private InsertManager insertManager;
    private MaxComputeMetrics maxComputeMetrics;
    private Instrumentation instrumentation;

    public MaxComputeClient(MaxComputeSinkConfig maxComputeSinkConfig,
                            Instrumentation instrumentation, MaxComputeMetrics maxComputeMetrics) {
        this.maxComputeSinkConfig = maxComputeSinkConfig;
        this.instrumentation = instrumentation;
        this.odps = initializeOdps();
        this.tableTunnel = new TableTunnel(odps);
        this.tableTunnel.setEndpoint(maxComputeSinkConfig.getMaxComputeTunnelUrl());
        this.maxComputeMetrics = maxComputeMetrics;
        this.insertManager = initializeInsertManager();
    }

    public void upsertTable(TableSchema tableSchema) throws OdpsException {
        String projectName = maxComputeSinkConfig.getMaxComputeProjectId();
        String datasetName = maxComputeSinkConfig.getMaxComputeSchema();
        String tableName = maxComputeSinkConfig.getMaxComputeTableName();
        if (!this.odps.tables().exists(tableName)) {
            createTable(tableSchema, projectName, datasetName, tableName);
            return;
        }
        Instant start = Instant.now();
        updateTable(tableSchema, projectName, datasetName, tableName);
        instrumentation.logInfo("Successfully updated maxCompute table " + tableName);
        instrument(start, MaxComputeMetrics.MaxComputeAPIType.TABLE_UPDATE);
    }

    private void createTable(TableSchema tableSchema, String projectName, String datasetName, String tableName) throws OdpsException {
        Instant start = Instant.now();
        this.odps.tables().create(projectName, datasetName, tableName, tableSchema, "",
                false, maxComputeSinkConfig.getMaxComputeTableLifecycleDays(),
                null, null);
        instrumentation.logInfo("Successfully created maxCompute table " + tableName);
        instrument(start, MaxComputeMetrics.MaxComputeAPIType.TABLE_CREATE);
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

    private void updateTable(TableSchema tableSchema, String projectName, String datasetName, String tableName) throws OdpsException {
        TableSchema oldSchema = this.odps.tables().get(projectName, datasetName, tableName)
                .getSchema();
        checkPartitionPrecondition(oldSchema);
        List<String> schemaDifferenceSql = SchemaDifferenceUtils.getSchemaDifferenceSql(oldSchema, tableSchema, datasetName, tableName);
        for (String sql : schemaDifferenceSql) {
            execute(sql);
        }
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

    public void insert(List<RecordWrapper> recordWrappers) throws TunnelException, IOException {
        insertManager.insert(recordWrappers);
    }

    private Odps initializeOdps() {
        Account account = new AliyunAccount(maxComputeSinkConfig.getMaxComputeAccessId(), maxComputeSinkConfig.getMaxComputeAccessKey());
        Odps odpsClient = new Odps(account);
        odpsClient.setDefaultProject(maxComputeSinkConfig.getMaxComputeProjectId());
        odpsClient.setEndpoint(maxComputeSinkConfig.getMaxComputeOdpsUrl());
        odpsClient.setCurrentSchema(maxComputeSinkConfig.getMaxComputeSchema());
        odpsClient.setGlobalSettings(getGlobalSettings());
        return odpsClient;
    }

    private InsertManager initializeInsertManager() {
        return InsertManagerFactory.createInsertManager(maxComputeSinkConfig, tableTunnel, instrumentation, maxComputeMetrics);
    }

    private Map<String, String> getGlobalSettings() {
        Map<String, String> globalSettings = new HashMap<>();
        globalSettings.put("setproject odps.schema.evolution.enable", "true");
        globalSettings.put("odps.namespace.schema", "true");
        return globalSettings;
    }

    public Instance execute(String sql) throws OdpsException {
        log.info("Executing SQL: {}", sql);
        return SQLTask.run(odps, sql);
    }

}
