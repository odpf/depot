package com.gotocompany.depot.maxcompute.client.insert;


import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.tunnel.io.CompressOption;
import com.gotocompany.depot.config.MaxComputeSinkConfig;
import com.gotocompany.depot.maxcompute.model.RecordWrapper;
import com.gotocompany.depot.metrics.Instrumentation;
import com.gotocompany.depot.metrics.MaxComputeMetrics;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class NonPartitionedInsertManagerTest {

    @Test
    public void shouldFlushAllTheRecords() throws IOException, TunnelException {
        TableTunnel.FlushResult flushResult = Mockito.mock(TableTunnel.FlushResult.class);
        Mockito.when(flushResult.getRecordCount())
                .thenReturn(2L);
        TableTunnel.StreamRecordPack streamRecordPack = Mockito.mock(TableTunnel.StreamRecordPack.class);
        TableTunnel.StreamUploadSession streamUploadSession = Mockito.spy(TableTunnel.StreamUploadSession.class);
        Mockito.when(streamRecordPack.flush(Mockito.any(TableTunnel.FlushOption.class)))
                .thenReturn(flushResult);
        Mockito.when(streamUploadSession.newRecordPack())
                .thenReturn(streamRecordPack);
        Mockito.when(streamRecordPack.flush())
                .thenReturn("traceId");
        TableTunnel tableTunnel = Mockito.mock(TableTunnel.class);
        TableTunnel.StreamUploadSession.Builder builder = Mockito.mock(TableTunnel.StreamUploadSession.Builder.class);
        Mockito.when(tableTunnel.buildStreamUploadSession(Mockito.anyString(), Mockito.anyString()))
                .thenReturn(builder);
        Mockito.when(builder.allowSchemaMismatch(Mockito.anyBoolean()))
                .thenReturn(builder);
        Mockito.when(builder.build())
                .thenReturn(streamUploadSession);
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        Mockito.when(maxComputeSinkConfig.getMaxComputeProjectId())
                .thenReturn("project");
        Mockito.when(maxComputeSinkConfig.getMaxComputeTableName())
                .thenReturn("table");
        Mockito.when(maxComputeSinkConfig.getMaxComputeRecordPackFlushTimeoutMs())
                .thenReturn(1000L);
        Instrumentation instrumentation = Mockito.mock(Instrumentation.class);
        Mockito.doNothing()
                .when(instrumentation)
                .captureCount(Mockito.anyString(), Mockito.anyLong());
        MaxComputeMetrics maxComputeMetrics = Mockito.mock(MaxComputeMetrics.class);
        Mockito.when(maxComputeMetrics.getMaxComputeFlushRecordMetric())
                .thenReturn("flush_record");
        Mockito.when(maxComputeMetrics.getMaxComputeFlushSizeMetric())
                .thenReturn("flush_size");
        NonPartitionedInsertManager nonPartitionedInsertManager = new NonPartitionedInsertManager(tableTunnel, maxComputeSinkConfig, instrumentation, maxComputeMetrics);
        List<RecordWrapper> recordWrappers = Collections.singletonList(
                Mockito.mock(RecordWrapper.class)
        );

        nonPartitionedInsertManager.insert(recordWrappers);

        Mockito.verify(streamRecordPack, Mockito.times(1))
                .flush(Mockito.any(TableTunnel.FlushOption.class));
    }

    @Test
    public void shouldFlushAllTheRecordsWithCompressOption() throws IOException, TunnelException {
        TableTunnel.FlushResult flushResult = Mockito.mock(TableTunnel.FlushResult.class);
        Mockito.when(flushResult.getRecordCount())
                .thenReturn(2L);
        TableTunnel.StreamRecordPack streamRecordPack = Mockito.mock(TableTunnel.StreamRecordPack.class);
        TableTunnel.StreamUploadSession streamUploadSession = Mockito.spy(TableTunnel.StreamUploadSession.class);
        Mockito.when(streamRecordPack.flush(Mockito.any(TableTunnel.FlushOption.class)))
                .thenReturn(flushResult);
        ArgumentCaptor<CompressOption> compressOptionArgumentCaptor = ArgumentCaptor.forClass(CompressOption.class);
        Mockito.when(streamUploadSession.newRecordPack(compressOptionArgumentCaptor.capture()))
                .thenReturn(streamRecordPack);
        Mockito.when(streamRecordPack.flush())
                .thenReturn("traceId");
        TableTunnel tableTunnel = Mockito.mock(TableTunnel.class);
        TableTunnel.StreamUploadSession.Builder builder = Mockito.mock(TableTunnel.StreamUploadSession.Builder.class);
        Mockito.when(tableTunnel.buildStreamUploadSession(Mockito.anyString(), Mockito.anyString()))
                .thenReturn(builder);
        Mockito.when(builder.allowSchemaMismatch(Mockito.anyBoolean()))
                .thenReturn(builder);
        Mockito.when(builder.build())
                .thenReturn(streamUploadSession);
        MaxComputeSinkConfig maxComputeSinkConfig = Mockito.mock(MaxComputeSinkConfig.class);
        Mockito.when(maxComputeSinkConfig.getMaxComputeProjectId())
                .thenReturn("project");
        Mockito.when(maxComputeSinkConfig.getMaxComputeTableName())
                .thenReturn("table");
        Mockito.when(maxComputeSinkConfig.getMaxComputeRecordPackFlushTimeoutMs())
                .thenReturn(1000L);
        Mockito.when(maxComputeSinkConfig.isStreamingInsertCompressEnabled())
                .thenReturn(true);
        Mockito.when(maxComputeSinkConfig.getMaxComputeCompressionAlgorithm())
                .thenReturn(CompressOption.CompressAlgorithm.ODPS_RAW);
        Mockito.when(maxComputeSinkConfig.getMaxComputeCompressionLevel())
                .thenReturn(1);
        Mockito.when(maxComputeSinkConfig.getMaxComputeCompressionStrategy())
                .thenReturn(1);
        Instrumentation instrumentation = Mockito.mock(Instrumentation.class);
        Mockito.doNothing()
                .when(instrumentation)
                .captureCount(Mockito.anyString(), Mockito.anyLong());
        MaxComputeMetrics maxComputeMetrics = Mockito.mock(MaxComputeMetrics.class);
        Mockito.when(maxComputeMetrics.getMaxComputeFlushRecordMetric())
                .thenReturn("flush_record");
        Mockito.when(maxComputeMetrics.getMaxComputeFlushSizeMetric())
                .thenReturn("flush_size");
        NonPartitionedInsertManager nonPartitionedInsertManager = new NonPartitionedInsertManager(tableTunnel, maxComputeSinkConfig, instrumentation, maxComputeMetrics);
        List<RecordWrapper> recordWrappers = Collections.singletonList(
                Mockito.mock(RecordWrapper.class)
        );

        nonPartitionedInsertManager.insert(recordWrappers);

        Mockito.verify(streamRecordPack, Mockito.times(1))
                .flush(Mockito.any(TableTunnel.FlushOption.class));
        Assertions.assertEquals(compressOptionArgumentCaptor.getValue().algorithm, CompressOption.CompressAlgorithm.ODPS_RAW);
        Assertions.assertEquals(compressOptionArgumentCaptor.getValue().strategy, 1);
        Assertions.assertEquals(compressOptionArgumentCaptor.getValue().level, 1);
    }

}
