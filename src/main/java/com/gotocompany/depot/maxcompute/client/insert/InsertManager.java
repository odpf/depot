package com.gotocompany.depot.maxcompute.client.insert;

import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.tunnel.io.CompressOption;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.maxcompute.model.RecordWrapper;

import java.io.IOException;
import java.util.List;

public interface InsertManager {
    void insert(List<RecordWrapper> recordWrappers) throws TunnelException, IOException;

    default TableTunnel.StreamRecordPack newRecordPack(TableTunnel.StreamUploadSession streamUploadSession,
                                                       MaxComputeSinkConfig maxComputeSinkConfig) throws IOException, TunnelException {
        if (!maxComputeSinkConfig.isStreamingInsertCompressEnabled()) {
            return streamUploadSession.newRecordPack();
        }
        return streamUploadSession.newRecordPack(new CompressOption(maxComputeSinkConfig.getMaxComputeCompressionAlgorithm(),
                maxComputeSinkConfig.getMaxComputeCompressionLevel(),
                maxComputeSinkConfig.getMaxComputeCompressionStrategy()));
    }
}
