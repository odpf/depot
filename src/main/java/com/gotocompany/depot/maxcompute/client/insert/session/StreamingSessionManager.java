package com.gotocompany.depot.maxcompute.client.insert.session;

import com.aliyun.odps.tunnel.TableTunnel;

import com.aliyun.odps.tunnel.TunnelException;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.metrics.Instrumentation;
import com.gotocompany.depot.metrics.MaxComputeMetrics;

import java.time.Instant;

public final class StreamingSessionManager {

    private final LoadingCache<String, TableTunnel.StreamUploadSession> sessionCache;

    private StreamingSessionManager(LoadingCache<String, TableTunnel.StreamUploadSession> loadingCache) {
        this.sessionCache = loadingCache;
    }

    public static StreamingSessionManager createNonPartitioned(TableTunnel tableTunnel,
                                                               MaxComputeSinkConfig maxComputeSinkConfig,
                                                               Instrumentation instrumentation,
                                                               MaxComputeMetrics maxComputeMetrics) {
        CacheLoader<String, TableTunnel.StreamUploadSession> cacheLoader = new CacheLoader<String, TableTunnel.StreamUploadSession>() {
            @Override
            public TableTunnel.StreamUploadSession load(String sessionId) throws TunnelException {
                Instant start = Instant.now();
                TableTunnel.StreamUploadSession streamUploadSession = tableTunnel.buildStreamUploadSession(
                                maxComputeSinkConfig.getMaxComputeProjectId(),
                                maxComputeSinkConfig.getMaxComputeTableName())
                        .allowSchemaMismatch(false)
                        .setSlotNum(maxComputeSinkConfig.getStreamingInsertTunnelSlotCountPerSession())
                        .build();
                instrumentation.captureDurationSince(maxComputeMetrics.getMaxComputeStreamingInsertSessionInitializationLatency(), start);
                instrumentation.incrementCounter(maxComputeMetrics.getMaxComputeStreamingInsertSessionCreatedCount());
                return streamUploadSession;
            }
        };
        return new StreamingSessionManager(CacheBuilder.newBuilder()
                .maximumSize(maxComputeSinkConfig.getStreamingInsertMaximumSessionCount())
                .build(cacheLoader));
    }

    public static StreamingSessionManager createPartitioned(TableTunnel tableTunnel,
                                                            MaxComputeSinkConfig maxComputeSinkConfig,
                                                            Instrumentation instrumentation,
                                                            MaxComputeMetrics maxComputeMetrics) {
        CacheLoader<String, TableTunnel.StreamUploadSession> cacheLoader = new CacheLoader<String, TableTunnel.StreamUploadSession>() {
            @Override
            public TableTunnel.StreamUploadSession load(String partitionSpecKey) throws TunnelException {
                Instant start = Instant.now();
                TableTunnel.StreamUploadSession streamUploadSession = tableTunnel.buildStreamUploadSession(
                                maxComputeSinkConfig.getMaxComputeProjectId(),
                                maxComputeSinkConfig.getMaxComputeTableName())
                        .setCreatePartition(true)
                        .setPartitionSpec(partitionSpecKey)
                        .allowSchemaMismatch(false)
                        .setSlotNum(maxComputeSinkConfig.getStreamingInsertTunnelSlotCountPerSession())
                        .build();
                instrumentation.captureDurationSince(maxComputeMetrics.getMaxComputeStreamingInsertSessionInitializationLatency(), start);
                instrumentation.incrementCounter(maxComputeMetrics.getMaxComputeStreamingInsertSessionCreatedCount());
                return streamUploadSession;
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
