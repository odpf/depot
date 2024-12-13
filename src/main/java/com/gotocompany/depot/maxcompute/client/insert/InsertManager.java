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

/**
 * InsertManager is responsible for inserting records into MaxCompute.
 */
@RequiredArgsConstructor
@Getter
public abstract class InsertManager {

    private final MaxComputeSinkConfig maxComputeSinkConfig;
    private final Instrumentation instrumentation;
    private final MaxComputeMetrics maxComputeMetrics;

    /**
     * Insert records into MaxCompute.
     * @param recordWrappers list of records to insert
     * @throws TunnelException if there is an error with the tunnel service, typically due to network issues
     * @throws IOException typically thrown when issues such as schema mismatch occur
     */
    public abstract void insert(List<RecordWrapper> recordWrappers) throws TunnelException, IOException;

    /**
     * Create a new record pack for streaming insert.
     * Record pack encloses the records to be inserted.
     *
     * @param streamUploadSession session for streaming insert
     * @return TableTunnel.StreamRecordPack
     * @throws IOException typically thrown when issues such as schema mismatch occur
     * @throws TunnelException if there is an error with the tunnel service, typically due to network issues
     */
    protected TableTunnel.StreamRecordPack newRecordPack(TableTunnel.StreamUploadSession streamUploadSession) throws IOException, TunnelException {
        if (!maxComputeSinkConfig.isStreamingInsertCompressEnabled()) {
            return streamUploadSession.newRecordPack();
        }
        return streamUploadSession.newRecordPack(new CompressOption(maxComputeSinkConfig.getMaxComputeCompressionAlgorithm(),
                maxComputeSinkConfig.getMaxComputeCompressionLevel(),
                maxComputeSinkConfig.getMaxComputeCompressionStrategy()));
    }

    /**
     * Instrument the insert operation.
     *
     * @param start start time of the operation
     * @param flushResult result of the flush operation
     */
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
