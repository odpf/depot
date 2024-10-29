package com.gotocompany.depot.maxcompute.client.insert;

import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.maxcompute.model.RecordWrapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.List;

@RequiredArgsConstructor
@Slf4j
public class NonPartitionedInsertManager implements InsertManager {

    private final TableTunnel tableTunnel;
    private final MaxComputeSinkConfig maxComputeSinkConfig;

    @Override
    public void insert(List<RecordWrapper> recordWrappers) throws TunnelException, IOException {
        TableTunnel.StreamUploadSession streamUploadSession = getStreamUploadSession();
        TableTunnel.StreamRecordPack recordPack = streamUploadSession.newRecordPack();
        for (RecordWrapper recordWrapper : recordWrappers) {
            recordPack.append(recordWrapper.getRecord());
        }
        TableTunnel.FlushResult flushResult = recordPack.flush(
                new TableTunnel.FlushOption()
                        .timeout(maxComputeSinkConfig.getMaxComputeRecordPackFlushTimeout()));
        log.info("Flushed {} records", flushResult.getRecordCount());
    }

    private TableTunnel.StreamUploadSession getStreamUploadSession() throws TunnelException {
        return tableTunnel.buildStreamUploadSession(maxComputeSinkConfig.getMaxComputeProjectId(),
                        maxComputeSinkConfig.getMaxComputeTableName())
                .build();
    }
}
