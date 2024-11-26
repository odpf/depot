package com.gotocompany.depot.maxcompute.client.insert.session;

import com.aliyun.odps.tunnel.TableTunnel;

import com.aliyun.odps.tunnel.TunnelException;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.gotocompany.depot.config.MaxComputeSinkConfig;

public class StreamingSessionManager {

    private final LoadingCache<String, TableTunnel.StreamUploadSession> sessionCache;

    private StreamingSessionManager(CacheLoader<String, TableTunnel.StreamUploadSession> cacheLoader,
                                   MaxComputeSinkConfig maxComputeSinkConfig) {
        sessionCache = CacheBuilder.newBuilder()
                .maximumSize(maxComputeSinkConfig.getStreamingInsertMaximumSessionCount())
                .build(cacheLoader);
    }

    public static StreamingSessionManager nonParititonedStreamingSessionManager(TableTunnel tableTunnel, MaxComputeSinkConfig maxComputeSinkConfig) {
        return new StreamingSessionManager(new CacheLoader<String, TableTunnel.StreamUploadSession>() {
            @Override
            public TableTunnel.StreamUploadSession load(String sessionId) throws TunnelException {
                return tableTunnel.buildStreamUploadSession(
                                maxComputeSinkConfig.getMaxComputeProjectId(),
                                maxComputeSinkConfig.getMaxComputeTableName())
                        .allowSchemaMismatch(false)
                        .build();
            }
        }, maxComputeSinkConfig);
    }

    public static StreamingSessionManager partitionedStreamingSessionManager(TableTunnel tableTunnel, MaxComputeSinkConfig maxComputeSinkConfig) {
        return new StreamingSessionManager(new CacheLoader<String, TableTunnel.StreamUploadSession>() {
            @Override
            public TableTunnel.StreamUploadSession load(String partitionSpecKey) throws TunnelException {
                return tableTunnel.buildStreamUploadSession(
                                maxComputeSinkConfig.getMaxComputeProjectId(),
                                maxComputeSinkConfig.getMaxComputeTableName())
                        .setCreatePartition(true)
                        .setPartitionSpec(partitionSpecKey)
                        .allowSchemaMismatch(false)
                        .build();
            }
        }, maxComputeSinkConfig);
    }

    public TableTunnel.StreamUploadSession getSession(String sessionId) {
        try {
            return sessionCache.get(sessionId);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void refreshSession(String sessionId) {
        sessionCache.refresh(sessionId);
    }

}
