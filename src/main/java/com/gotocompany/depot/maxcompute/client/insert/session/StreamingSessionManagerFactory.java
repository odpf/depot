package com.gotocompany.depot.maxcompute.client.insert.session;

import com.aliyun.odps.tunnel.TableTunnel;
import com.gotocompany.depot.config.MaxComputeSinkConfig;

public class StreamingSessionManagerFactory {

    public static StreamingSessionManager createStreamingSessionManager(
            TableTunnel tableTunnel,
            MaxComputeSinkConfig maxComputeSinkConfig
    ) {
        if (maxComputeSinkConfig.isTablePartitioningEnabled()) {
            return new PartitionedStreamingSessionManager(tableTunnel, maxComputeSinkConfig);
        }
        return new NonPartitionedStreamingSessionManager(tableTunnel, maxComputeSinkConfig);
    }

}
