package com.gotocompany.depot.maxcompute.client.insert;

import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.maxcompute.model.RecordWrapper;
import com.gotocompany.depot.metrics.Instrumentation;
import com.gotocompany.depot.metrics.MaxComputeMetrics;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.time.Instant;
import java.util.List;

@Slf4j
public class NonPartitionedInsertManager extends InsertManager {

    public NonPartitionedInsertManager(TableTunnel tableTunnel, MaxComputeSinkConfig maxComputeSinkConfig, Instrumentation instrumentation, MaxComputeMetrics maxComputeMetrics) {
        super(tableTunnel, maxComputeSinkConfig, instrumentation, maxComputeMetrics);
    }

    @Override
    public void insert(List<RecordWrapper> recordWrappers) throws TunnelException, IOException {
        TableTunnel.StreamUploadSession streamUploadSession = getStreamUploadSession();
        TableTunnel.StreamRecordPack recordPack = newRecordPack(streamUploadSession);
        for (RecordWrapper recordWrapper : recordWrappers) {
            recordPack.append(recordWrapper.getRecord());
        }
        Instant start = Instant.now();
        TableTunnel.FlushResult flushResult = recordPack.flush(
                new TableTunnel.FlushOption()
                        .timeout(super.getMaxComputeSinkConfig().getMaxComputeRecordPackFlushTimeoutMs()));
        instrument(start, flushResult);
    }

    private TableTunnel.StreamUploadSession getStreamUploadSession() throws TunnelException {
        return super.getTableTunnel().buildStreamUploadSession(
                        super.getMaxComputeSinkConfig().getMaxComputeProjectId(),
                        super.getMaxComputeSinkConfig().getMaxComputeTableName())
                .allowSchemaMismatch(false)
                .build();
    }

}
