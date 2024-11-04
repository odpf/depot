package com.gotocompany.depot.maxcompute.client;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.maxcompute.client.insert.InsertManager;
import com.gotocompany.depot.maxcompute.client.insert.InsertManagerFactory;
import com.gotocompany.depot.maxcompute.model.RecordWrapper;
import com.gotocompany.depot.metrics.Instrumentation;
import com.gotocompany.depot.metrics.MaxComputeMetrics;
import com.gotocompany.depot.metrics.StatsDReporter;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@AllArgsConstructor
@NoArgsConstructor
public class MaxComputeClient {

    private Odps odps;
    private MaxComputeSinkConfig maxComputeSinkConfig;
    private TableTunnel tableTunnel;
    private InsertManager insertManager;
    private StatsDReporter statsDReporter;
    private MaxComputeMetrics maxComputeMetrics;

    public MaxComputeClient(MaxComputeSinkConfig maxComputeSinkConfig,
                            StatsDReporter statsDReporter,
                            MaxComputeMetrics maxComputeMetrics) {
        this.maxComputeSinkConfig = maxComputeSinkConfig;
        this.odps = initializeOdps();
        this.tableTunnel = new TableTunnel(odps);
        this.tableTunnel.setEndpoint(maxComputeSinkConfig.getMaxComputeTunnelUrl());
        this.statsDReporter = statsDReporter;
        this.maxComputeMetrics = maxComputeMetrics;
        this.insertManager = initializeInsertManager();
    }

    public void upsertTable(TableSchema tableSchema) throws OdpsException {
        String tableName = maxComputeSinkConfig.getMaxComputeTableName();
        if (!this.odps.tables().exists(tableName)) {
            this.odps.tables().create(odps.getDefaultProject(), tableName, tableSchema, "",
                    false, maxComputeSinkConfig.getMaxComputeTableLifecycleDays(),
                    null, null);
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
        return InsertManagerFactory.createInsertManager(maxComputeSinkConfig, tableTunnel,
                new Instrumentation(statsDReporter, InsertManager.class), maxComputeMetrics);
    }

    private Map<String, String> getGlobalSettings() {
        Map<String, String> globalSettings = new HashMap<>();
        globalSettings.put("setproject odps.schema.evolution.enable", "true");
        globalSettings.put("odps.namespace.schema", "true");
        return globalSettings;
    }

}
