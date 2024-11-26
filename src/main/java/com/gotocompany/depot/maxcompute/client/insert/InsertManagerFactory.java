package com.gotocompany.depot.maxcompute.client.insert;

import com.aliyun.odps.tunnel.TableTunnel;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.maxcompute.client.insert.session.StreamingSessionManager;
import com.gotocompany.depot.metrics.Instrumentation;
import com.gotocompany.depot.metrics.MaxComputeMetrics;

public class InsertManagerFactory {

    public static InsertManager createInsertManager(MaxComputeSinkConfig maxComputeSinkConfig,
                                                    TableTunnel tableTunnel,
                                                    Instrumentation instrumentation,
                                                    MaxComputeMetrics maxComputeMetrics) {
        StreamingSessionManager streamingSessionManager = maxComputeSinkConfig.isTablePartitioningEnabled()
                ? StreamingSessionManager.partitionedStreamingSessionManager(tableTunnel, maxComputeSinkConfig) : StreamingSessionManager.nonParititonedStreamingSessionManager(tableTunnel, maxComputeSinkConfig);
        if (maxComputeSinkConfig.isTablePartitioningEnabled()) {
            return new PartitionedInsertManager(tableTunnel, maxComputeSinkConfig, instrumentation, maxComputeMetrics, streamingSessionManager);
        } else {
            return new NonPartitionedInsertManager(tableTunnel, maxComputeSinkConfig, instrumentation, maxComputeMetrics, streamingSessionManager);
        }
    }

}
