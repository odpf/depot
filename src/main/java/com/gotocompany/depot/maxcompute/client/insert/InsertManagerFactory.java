package com.gotocompany.depot.maxcompute.client.insert;

import com.aliyun.odps.tunnel.TableTunnel;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.maxcompute.client.insert.session.StreamingSessionManager;
import com.gotocompany.depot.metrics.Instrumentation;
import com.gotocompany.depot.metrics.MaxComputeMetrics;

/**
 * Factory class to create InsertManager based on the configuration.
 */
public class InsertManagerFactory {

    /**
     * Creates an InsertManager based on the configuration.
     * PartitionedInsertManager is created if table partitioning is enabled. Otherwise, NonPartitionedInsertManager is created.
     * Each InsertManager uses a StreamingSessionManager to manage the streaming session.
     *
     * @param maxComputeSinkConfig configuration for MaxCompute sink
     * @param tableTunnel tunnel service for MaxCompute
     * @param instrumentation metrics instrumentation
     * @param maxComputeMetrics metrics for MaxCompute
     * @return InsertManager
     */
    public static InsertManager createInsertManager(MaxComputeSinkConfig maxComputeSinkConfig,
                                                    TableTunnel tableTunnel,
                                                    Instrumentation instrumentation,
                                                    MaxComputeMetrics maxComputeMetrics) {
        StreamingSessionManager streamingSessionManager = getStreamingSessionManager(maxComputeSinkConfig, tableTunnel, instrumentation, maxComputeMetrics);
        if (maxComputeSinkConfig.isTablePartitioningEnabled()) {
            return new PartitionedInsertManager(maxComputeSinkConfig, instrumentation, maxComputeMetrics, streamingSessionManager);
        } else {
            return new NonPartitionedInsertManager(maxComputeSinkConfig, instrumentation, maxComputeMetrics, streamingSessionManager);
        }
    }

    private static StreamingSessionManager getStreamingSessionManager(MaxComputeSinkConfig maxComputeSinkConfig,
                                                                      TableTunnel tableTunnel,
                                                                      Instrumentation instrumentation,
                                                                      MaxComputeMetrics maxComputeMetrics) {
        StreamingSessionManager streamingSessionManager;
        if (maxComputeSinkConfig.isTablePartitioningEnabled()) {
            streamingSessionManager = StreamingSessionManager.createPartitioned(tableTunnel, maxComputeSinkConfig, instrumentation, maxComputeMetrics);
        } else {
            streamingSessionManager = StreamingSessionManager.createNonPartitioned(tableTunnel, maxComputeSinkConfig, instrumentation, maxComputeMetrics);
        }
        return streamingSessionManager;
    }

}
