package com.gotocompany.depot.maxcompute.client.insert.session;

import com.aliyun.odps.tunnel.TableTunnel;

import com.aliyun.odps.tunnel.TunnelException;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.metrics.Instrumentation;
import com.gotocompany.depot.metrics.MaxComputeMetrics;

public final class StreamingSessionManager {

    private final LoadingCache<String, TableTunnel.StreamUploadSession> sessionCache;
    private final Instrumentation instrumentation;
    private final MaxComputeMetrics maxComputeMetrics;

    private StreamingSessionManager(LoadingCache<String, TableTunnel.StreamUploadSession> loadingCache,
                                    Instrumentation instrumentation,
                                    MaxComputeMetrics maxComputeMetrics
                                    ) {
        this.sessionCache = loadingCache;
        this.instrumentation = instrumentation;
        this.maxComputeMetrics = maxComputeMetrics;
    }

    public static StreamingSessionManager createNonPartitioned(TableTunnel tableTunnel,
                                                               MaxComputeSinkConfig maxComputeSinkConfig,
                                                               Instrumentation instrumentation,
                                                               MaxComputeMetrics maxComputeMetrics) {
        CacheLoader<String, TableTunnel.StreamUploadSession> cacheLoader = new CacheLoader<String, TableTunnel.StreamUploadSession>() {
            @Override
            public TableTunnel.StreamUploadSession load(String sessionId) throws TunnelException {
                return tableTunnel.buildStreamUploadSession(
                                maxComputeSinkConfig.getMaxComputeProjectId(),
                                maxComputeSinkConfig.getMaxComputeTableName())
                        .allowSchemaMismatch(false)
                        .setSlotNum(maxComputeSinkConfig.getStreamingInsertTunnelSlotCountPerSession())
                        .build();
            }
        };
        return new StreamingSessionManager(CacheBuilder.newBuilder()
                .maximumSize(maxComputeSinkConfig.getStreamingInsertMaximumSessionCount())
                .build(cacheLoader), instrumentation, maxComputeMetrics);
    }

    public static StreamingSessionManager createPartitioned(TableTunnel tableTunnel,
                                                            MaxComputeSinkConfig maxComputeSinkConfig,
                                                            Instrumentation instrumentation,
                                                            MaxComputeMetrics maxComputeMetrics) {
        CacheLoader<String, TableTunnel.StreamUploadSession> cacheLoader = new CacheLoader<String, TableTunnel.StreamUploadSession>() {
            @Override
            public TableTunnel.StreamUploadSession load(String partitionSpecKey) throws TunnelException {
                return tableTunnel.buildStreamUploadSession(
                                maxComputeSinkConfig.getMaxComputeProjectId(),
                                maxComputeSinkConfig.getMaxComputeTableName())
                        .setCreatePartition(true)
                        .setPartitionSpec(partitionSpecKey)
                        .allowSchemaMismatch(false)
                        .setSlotNum(maxComputeSinkConfig.getStreamingInsertTunnelSlotCountPerSession())
                        .build();
            }
        };
        return new StreamingSessionManager(CacheBuilder.newBuilder()
                .maximumSize(maxComputeSinkConfig.getStreamingInsertMaximumSessionCount())
                .build(cacheLoader), instrumentation, maxComputeMetrics);
    }

    public TableTunnel.StreamUploadSession getSession(String sessionId) {
        instrumentation.captureCount(maxComputeMetrics.getMaxComputeStreamingInsertSessionCount(), sessionCache.size(),
                String.format(MaxComputeMetrics.MAXCOMPUTE_SINK_THREAD_TAG, Thread.currentThread().getName()));
        return sessionCache.getUnchecked(sessionId);
    }

    public void refreshSession(String sessionId) {
        sessionCache.refresh(sessionId);
    }

}
