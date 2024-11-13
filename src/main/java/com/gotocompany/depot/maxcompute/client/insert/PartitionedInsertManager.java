package com.gotocompany.depot.maxcompute.client.insert;

import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.maxcompute.model.RecordWrapper;
import com.gotocompany.depot.metrics.Instrumentation;
import com.gotocompany.depot.metrics.MaxComputeMetrics;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@RequiredArgsConstructor
@Slf4j
public class PartitionedInsertManager implements InsertManager {

    private final TableTunnel tableTunnel;
    private final MaxComputeSinkConfig maxComputeSinkConfig;
    private final Instrumentation instrumentation;
    private final MaxComputeMetrics maxComputeMetrics;

    @Override
    public void insert(List<RecordWrapper> recordWrappers) throws TunnelException, IOException {
        Map<String, List<RecordWrapper>> partitionSpecRecordWrapperMap = recordWrappers.stream()
                .collect(Collectors.groupingBy(record -> record.getPartitionSpec().toString()));
        for (Map.Entry<String, List<RecordWrapper>> entry : partitionSpecRecordWrapperMap.entrySet()) {
            TableTunnel.StreamUploadSession streamUploadSession = getStreamUploadSession(entry.getValue().get(0).getPartitionSpec());
            TableTunnel.StreamRecordPack recordPack = newRecordPack(streamUploadSession, maxComputeSinkConfig);
            for (RecordWrapper recordWrapper : entry.getValue()) {
                recordPack.append(recordWrapper.getRecord());
            }
            TableTunnel.FlushResult flushResult = recordPack.flush(
                    new TableTunnel.FlushOption()
                            .timeout(maxComputeSinkConfig.getMaxComputeRecordPackFlushTimeoutMs()));
            instrumentation.captureCount(maxComputeMetrics.getMaxComputeFlushRecordMetric(), flushResult.getRecordCount());
            instrumentation.captureCount(maxComputeMetrics.getMaxComputeFlushSizeMetric(), flushResult.getFlushSize());
        }
    }

    private TableTunnel.StreamUploadSession getStreamUploadSession(PartitionSpec partitionSpec) throws TunnelException {
        return tableTunnel.buildStreamUploadSession(maxComputeSinkConfig.getMaxComputeProjectId(),
                        maxComputeSinkConfig.getMaxComputeTableName())
                .setCreatePartition(true)
                .setPartitionSpec(partitionSpec)
                .allowSchemaMismatch(false)
                .build();
    }

}
