package com.gotocompany.depot.maxcompute.client.insert.session;

import com.aliyun.odps.tunnel.TableTunnel;

import com.aliyun.odps.tunnel.TunnelException;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.gotocompany.depot.config.MaxComputeSinkConfig;

public final class StreamingSessionManager {

    private final LoadingCache<String, TableTunnel.StreamUploadSession> sessionCache;

    private StreamingSessionManager(LoadingCache<String, TableTunnel.StreamUploadSession> loadingCache) {
        sessionCache = loadingCache;
    }

    public static StreamingSessionManager createNonPartitioned(TableTunnel tableTunnel, MaxComputeSinkConfig maxComputeSinkConfig) {
        CacheLoader<String, TableTunnel.StreamUploadSession> cacheLoader = new CacheLoader<String, TableTunnel.StreamUploadSession>() {
            @Override
            public TableTunnel.StreamUploadSession load(String sessionId) throws TunnelException {
                return tableTunnel.buildStreamUploadSession(
                                maxComputeSinkConfig.getMaxComputeProjectId(),
                                maxComputeSinkConfig.getMaxComputeTableName())
                        .allowSchemaMismatch(false)
                        .build();
            }
        };
        return new StreamingSessionManager(CacheBuilder.newBuilder()
                .maximumSize(maxComputeSinkConfig.getStreamingInsertMaximumSessionCount())
                .build(cacheLoader));
    }

    public static StreamingSessionManager createPartitioned(TableTunnel tableTunnel, MaxComputeSinkConfig maxComputeSinkConfig) {
        CacheLoader<String, TableTunnel.StreamUploadSession> cacheLoader = new CacheLoader<String, TableTunnel.StreamUploadSession>() {
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
        };
        return new StreamingSessionManager(CacheBuilder.newBuilder()
                .maximumSize(maxComputeSinkConfig.getStreamingInsertMaximumSessionCount())
                .build(cacheLoader));
    }

    public TableTunnel.StreamUploadSession getSession(String sessionId) {
        return sessionCache.getUnchecked(sessionId);
    }

    public void refreshSession(String sessionId) {
        sessionCache.refresh(sessionId);
    }

}
