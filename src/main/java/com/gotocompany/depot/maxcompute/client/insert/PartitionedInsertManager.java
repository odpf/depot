package com.gotocompany.depot.maxcompute.client.insert;

import com.aliyun.odps.PartitionSpec;
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
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
public class PartitionedInsertManager extends InsertManager {

    public PartitionedInsertManager(TableTunnel tableTunnel, MaxComputeSinkConfig maxComputeSinkConfig, Instrumentation instrumentation, MaxComputeMetrics maxComputeMetrics) {
        super(tableTunnel, maxComputeSinkConfig, instrumentation, maxComputeMetrics);
    }

    @Override
    public void insert(List<RecordWrapper> recordWrappers) throws TunnelException, IOException {
        Map<String, List<RecordWrapper>> partitionSpecRecordWrapperMap = recordWrappers.stream()
                .collect(Collectors.groupingBy(record -> record.getPartitionSpec().toString()));
        for (Map.Entry<String, List<RecordWrapper>> entry : partitionSpecRecordWrapperMap.entrySet()) {
            TableTunnel.StreamUploadSession streamUploadSession = getStreamUploadSession(entry.getValue().get(0).getPartitionSpec());
            TableTunnel.StreamRecordPack recordPack = newRecordPack(streamUploadSession);
            for (RecordWrapper recordWrapper : entry.getValue()) {
                recordPack.append(recordWrapper.getRecord());
            }
            Instant start = Instant.now();
            TableTunnel.FlushResult flushResult = recordPack.flush(
                    new TableTunnel.FlushOption()
                            .timeout(super.getMaxComputeSinkConfig().getMaxComputeRecordPackFlushTimeoutMs()));
            instrument(start, flushResult);
        }
    }

    private TableTunnel.StreamUploadSession getStreamUploadSession(PartitionSpec partitionSpec) throws TunnelException {
        return super.getTableTunnel().buildStreamUploadSession(super.getMaxComputeSinkConfig().getMaxComputeProjectId(),
                        super.getMaxComputeSinkConfig().getMaxComputeTableName())
                .setCreatePartition(true)
                .setPartitionSpec(partitionSpec)
                .allowSchemaMismatch(false)
                .build();
    }

}
