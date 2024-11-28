package com.gotocompany.depot.maxcompute.client.insert;

import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.tunnel.io.CompressOption;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.maxcompute.model.RecordWrapper;
import com.gotocompany.depot.metrics.Instrumentation;
import com.gotocompany.depot.metrics.MaxComputeMetrics;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.io.IOException;
import java.time.Instant;
import java.util.List;

@RequiredArgsConstructor
@Getter
public abstract class InsertManager {

    private final MaxComputeSinkConfig maxComputeSinkConfig;
    private final Instrumentation instrumentation;
    private final MaxComputeMetrics maxComputeMetrics;

    public abstract void insert(List<RecordWrapper> recordWrappers) throws TunnelException, IOException;

    protected TableTunnel.StreamRecordPack newRecordPack(TableTunnel.StreamUploadSession streamUploadSession) throws IOException, TunnelException {
        if (!maxComputeSinkConfig.isStreamingInsertCompressEnabled()) {
            return streamUploadSession.newRecordPack();
        }
        return streamUploadSession.newRecordPack(new CompressOption(maxComputeSinkConfig.getMaxComputeCompressionAlgorithm(),
                maxComputeSinkConfig.getMaxComputeCompressionLevel(),
                maxComputeSinkConfig.getMaxComputeCompressionStrategy()));
    }

    protected void instrument(Instant start, TableTunnel.FlushResult flushResult) {
        instrumentation.incrementCounter(maxComputeMetrics.getMaxComputeOperationTotalMetric(),
                String.format(MaxComputeMetrics.MAXCOMPUTE_API_TAG, MaxComputeMetrics.MaxComputeAPIType.TABLE_INSERT));
        instrumentation.captureDurationSince(maxComputeMetrics.getMaxComputeOperationLatencyMetric(), start,
                String.format(MaxComputeMetrics.MAXCOMPUTE_API_TAG, MaxComputeMetrics.MaxComputeAPIType.TABLE_INSERT));
        instrumentation.captureCount(maxComputeMetrics.getMaxComputeFlushRecordMetric(), flushResult.getRecordCount(),
                String.format(MaxComputeMetrics.MAXCOMPUTE_COMPRESSION_TAG, maxComputeSinkConfig.isStreamingInsertCompressEnabled(), maxComputeSinkConfig.getMaxComputeCompressionAlgorithm()));
        instrumentation.captureCount(maxComputeMetrics.getMaxComputeFlushSizeMetric(), flushResult.getFlushSize(),
                String.format(MaxComputeMetrics.MAXCOMPUTE_COMPRESSION_TAG, maxComputeSinkConfig.isStreamingInsertCompressEnabled(), maxComputeSinkConfig.getMaxComputeCompressionAlgorithm()));
    }

}
