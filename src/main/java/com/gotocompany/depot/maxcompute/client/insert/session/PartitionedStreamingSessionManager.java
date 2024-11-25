package com.gotocompany.depot.maxcompute.client.insert.session;

import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import lombok.RequiredArgsConstructor;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@RequiredArgsConstructor
public class PartitionedStreamingSessionManager implements StreamingSessionManager {

    private final Map<String, TableTunnel.StreamUploadSession> sessionMap;
    private final TableTunnel tableTunnel;
    private final MaxComputeSinkConfig maxComputeSinkConfig;

    public PartitionedStreamingSessionManager(TableTunnel tableTunnel, MaxComputeSinkConfig maxComputeSinkConfig) {
        this.sessionMap = new ConcurrentHashMap<>();
        this.tableTunnel = tableTunnel;
        this.maxComputeSinkConfig = maxComputeSinkConfig;
    }

    @Override
    public TableTunnel.StreamUploadSession getSession(String sessionId) throws TunnelException {
        if (!sessionMap.containsKey(sessionId)) {
            buildNewSession(sessionId);
        }
        return sessionMap.get(sessionId);
    }

    @Override
    public void clearSession(String sessionId) {
        sessionMap.remove(sessionId);
    }

    private void buildNewSession(String sessionId) throws TunnelException {
        TableTunnel.StreamUploadSession streamUploadSession = tableTunnel.buildStreamUploadSession(
                        maxComputeSinkConfig.getMaxComputeProjectId(),
                        maxComputeSinkConfig.getMaxComputeTableName())
                .setCreatePartition(true)
                .setPartitionSpec(sessionId)
                .allowSchemaMismatch(false)
                .build();
        sessionMap.put(sessionId, streamUploadSession);
    }

}
