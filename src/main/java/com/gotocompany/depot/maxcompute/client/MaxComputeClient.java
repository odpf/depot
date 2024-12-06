package com.gotocompany.depot.maxcompute.client;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.maxcompute.client.ddl.DdlManager;
import com.gotocompany.depot.maxcompute.client.insert.InsertManager;
import com.gotocompany.depot.maxcompute.client.insert.InsertManagerFactory;
import com.gotocompany.depot.maxcompute.model.RecordWrapper;
import com.gotocompany.depot.metrics.Instrumentation;
import com.gotocompany.depot.metrics.MaxComputeMetrics;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.List;

@AllArgsConstructor
@NoArgsConstructor
@Slf4j
public class MaxComputeClient {

    private Odps odps;
    private MaxComputeSinkConfig maxComputeSinkConfig;
    private TableTunnel tableTunnel;
    private InsertManager insertManager;
    private DdlManager ddlManager;
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
        this.ddlManager = initializeDdlManager();
    }

    public TableSchema getLatestTableSchema() {
        return odps.tables()
                .get(maxComputeSinkConfig.getMaxComputeProjectId(),
                        maxComputeSinkConfig.getMaxComputeSchema(),
                        maxComputeSinkConfig.getMaxComputeTableName())
                .getSchema();
    }

    public void createOrUpdateTable(TableSchema tableSchema) throws OdpsException {
        ddlManager.createOrUpdateTable(tableSchema);
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
        odpsClient.setGlobalSettings(maxComputeSinkConfig.getOdpsGlobalSettings());
        return odpsClient;
    }

    private InsertManager initializeInsertManager() {
        return InsertManagerFactory.createInsertManager(maxComputeSinkConfig, tableTunnel, instrumentation, maxComputeMetrics);
    }

    private DdlManager initializeDdlManager() {
        return new DdlManager(odps, maxComputeSinkConfig, instrumentation, maxComputeMetrics);
    }

}
