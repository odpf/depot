package com.gotocompany.depot.maxcompute.client.insert.session;

import com.aliyun.odps.tunnel.TableTunnel;

import com.aliyun.odps.tunnel.TunnelException;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.metrics.Instrumentation;
import com.gotocompany.depot.metrics.MaxComputeMetrics;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

import java.time.Instant;

public final class StreamingSessionManager {

    private final LoadingCache<StreamingSessionCacheKey, TableTunnel.StreamUploadSession> sessionCache;

    private StreamingSessionManager(LoadingCache<StreamingSessionCacheKey, TableTunnel.StreamUploadSession> loadingCache) {
        this.sessionCache = loadingCache;
    }

    public static StreamingSessionManager createNonPartitioned(TableTunnel tableTunnel,
                                                               MaxComputeSinkConfig maxComputeSinkConfig,
                                                               Instrumentation instrumentation,
                                                               MaxComputeMetrics maxComputeMetrics) {
        CacheLoader<StreamingSessionCacheKey, TableTunnel.StreamUploadSession> cacheLoader = new CacheLoader<StreamingSessionCacheKey, TableTunnel.StreamUploadSession>() {
            @Override
            public TableTunnel.StreamUploadSession load(StreamingSessionCacheKey cacheKey) throws TunnelException {
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
        CacheLoader<StreamingSessionCacheKey, TableTunnel.StreamUploadSession> cacheLoader = new CacheLoader<StreamingSessionCacheKey, TableTunnel.StreamUploadSession>() {
            @Override
            public TableTunnel.StreamUploadSession load(StreamingSessionCacheKey cacheKey) throws TunnelException {
                Instant start = Instant.now();
                TableTunnel.StreamUploadSession streamUploadSession = tableTunnel.buildStreamUploadSession(
                                maxComputeSinkConfig.getMaxComputeProjectId(),
                                maxComputeSinkConfig.getMaxComputeTableName())
                        .setCreatePartition(true)
                        .setPartitionSpec(cacheKey.getPartitionSpecKey())
                        .allowSchemaMismatch(false)
                        .setSlotNum(maxComputeSinkConfig.getStreamingInsertTunnelSlotCountPerSession())
                        .build();
                instrumentation.captureDurationSince(maxComputeMetrics.getMaxComputeStreamingInsertSessionInitializationLatency(), start);
                instrumentation.incrementCounter(maxComputeMetrics.getMaxComputeStreamingInsertSessionCreatedCount());
                instrumentation.logInfo("Created session {} by thread {}", cacheKey, Thread.currentThread().getName());
                return streamUploadSession;
            }
        };
        return new StreamingSessionManager(CacheBuilder.newBuilder()
                .maximumSize(maxComputeSinkConfig.getStreamingInsertMaximumSessionCount())
                .build(cacheLoader));
    }

    public TableTunnel.StreamUploadSession getSession(StreamingSessionCacheKey cacheKey) {
        return sessionCache.getUnchecked(cacheKey);
    }

    public void refreshSession(StreamingSessionCacheKey streamingSessionCacheKey) {
        sessionCache.refresh(streamingSessionCacheKey);
    }

    @RequiredArgsConstructor
    @Getter
    @EqualsAndHashCode
    @ToString
    public static class StreamingSessionCacheKey {
        private final String partitionSpecKey;
        private final String processId;
    }
}
