package com.gotocompany.depot.maxcompute.client.insert;

import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.maxcompute.client.insert.session.StreamingSessionManager;
import com.gotocompany.depot.maxcompute.model.RecordWrapper;
import com.gotocompany.depot.metrics.Instrumentation;
import com.gotocompany.depot.metrics.MaxComputeMetrics;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.time.Instant;
import java.util.List;

/**
 * NonPartitionedInsertManager is responsible for inserting non-partitioned records into MaxCompute.
 */
@Slf4j
public class NonPartitionedInsertManager extends InsertManager {

    private static final String NON_PARTITIONED = "non-partitioned";
    private final StreamingSessionManager streamingSessionManager;
    private final TableTunnel.FlushOption flushOption;

    public NonPartitionedInsertManager(MaxComputeSinkConfig maxComputeSinkConfig,
                                       Instrumentation instrumentation,
                                       MaxComputeMetrics maxComputeMetrics,
                                       StreamingSessionManager streamingSessionManager) {
        super(maxComputeSinkConfig, instrumentation, maxComputeMetrics);
        this.streamingSessionManager = streamingSessionManager;
        this.flushOption = new TableTunnel.FlushOption()
                .timeout(super.getMaxComputeSinkConfig().getMaxComputeRecordPackFlushTimeoutMs());
    }

    /**
     * Insert records into MaxCompute.
     * Each thread will have its own StreamUploadSession.
     *
     * @param recordWrappers list of records to insert
     * @throws TunnelException if there is an error with the tunnel service, typically due to network issues
     * @throws IOException typically thrown when issues such as schema mismatch occur
     */
    @Override
    public void insert(List<RecordWrapper> recordWrappers) throws TunnelException, IOException {
        TableTunnel.StreamUploadSession streamUploadSession = streamingSessionManager.getSession(NON_PARTITIONED);
        TableTunnel.StreamRecordPack recordPack = newRecordPack(streamUploadSession);
        for (RecordWrapper recordWrapper : recordWrappers) {
            try {
                recordPack.append(recordWrapper.getRecord());
            } catch (IOException e) {
                log.error("Schema Mismatch, rebuilding the session", e);
                streamingSessionManager.refreshSession(NON_PARTITIONED);
                throw e;
            }
        }
        Instant start = Instant.now();
        TableTunnel.FlushResult flushResult = recordPack.flush(flushOption);
        instrument(start, flushResult);
    }

}
