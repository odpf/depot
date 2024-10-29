package com.gotocompany.depot.maxcompute.client.insert;

import com.aliyun.odps.tunnel.TableTunnel;
import com.gotocompany.depot.config.MaxComputeSinkConfig;

public class InsertManagerFactory {

    public static InsertManager createInsertManager(MaxComputeSinkConfig maxComputeSinkConfig,
                                                    TableTunnel tableTunnel) {
        if (maxComputeSinkConfig.isTablePartitioningEnabled()) {
            return new PartitionedInsertManager(tableTunnel, maxComputeSinkConfig);
        } else {
            return new NonPartitionedInsertManager(tableTunnel, maxComputeSinkConfig);
        }
    }

}
