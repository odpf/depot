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

@Slf4j
public class NonPartitionedInsertManager extends InsertManager {

    private static final String NON_PARTITIONED = "non-partitioned";
    private final StreamingSessionManager streamingSessionManager;

    public NonPartitionedInsertManager(TableTunnel tableTunnel,
                                       MaxComputeSinkConfig maxComputeSinkConfig,
                                       Instrumentation instrumentation,
                                       MaxComputeMetrics maxComputeMetrics,
                                       StreamingSessionManager streamingSessionManager) {
        super(tableTunnel, maxComputeSinkConfig, instrumentation, maxComputeMetrics);
        this.streamingSessionManager = streamingSessionManager;
    }

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
        TableTunnel.FlushResult flushResult = recordPack.flush(
                new TableTunnel.FlushOption()
                        .timeout(super.getMaxComputeSinkConfig().getMaxComputeRecordPackFlushTimeoutMs()));
        instrument(start, flushResult);
    }


}
