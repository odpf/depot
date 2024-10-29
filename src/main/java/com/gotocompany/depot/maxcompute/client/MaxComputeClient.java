package com.gotocompany.depot.maxcompute.client;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.tunnel.TableTunnel;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.maxcompute.client.insert.InsertManager;
import com.gotocompany.depot.maxcompute.client.insert.InsertManagerFactory;
import com.gotocompany.depot.maxcompute.model.RecordWrapper;
import lombok.NoArgsConstructor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@NoArgsConstructor
public class MaxComputeClient {

    private Odps odps;
    private MaxComputeSinkConfig maxComputeSinkConfig;
    private TableTunnel tableTunnel;
    private InsertManager insertManager;

    public MaxComputeClient(MaxComputeSinkConfig maxComputeSinkConfig) {
        this.maxComputeSinkConfig = maxComputeSinkConfig;
        this.odps = initializeOdps();
        this.tableTunnel = new TableTunnel(odps);
        this.tableTunnel.setEndpoint(maxComputeSinkConfig.getMaxComputeTunnelUrl());
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

    public void insert(List<RecordWrapper> recordWrappers) {
        try {
            insertManager.insert(recordWrappers);
        } catch (Exception e) {
            throw new RuntimeException("Failed to insert records into MaxCompute", e);
        }
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
        return InsertManagerFactory.createInsertManager(maxComputeSinkConfig, tableTunnel);
    }

    private Map<String, String> getGlobalSettings() {
        Map<String, String> globalSettings = new HashMap<>();
        globalSettings.put("setproject odps.schema.evolution.enable", "true");
        globalSettings.put("odps.namespace.schema", "true");
        return globalSettings;
    }

}
